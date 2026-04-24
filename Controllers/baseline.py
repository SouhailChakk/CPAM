import socket
import ipaddress
from time import time
from typing import Dict, List, Optional, Set, Tuple
from collections import deque

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import arp, ethernet, ether_types, icmp, ipv4, packet, tcp, udp
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_link, get_switch


class BaselineReactiveController(app_manager.RyuApp):
    """
    Baseline Ryu controller for fair comparison against the CPAM/MTD controller.

    Design goals:
      - Compatible with the same industry topology (4/24/48-port variants)
      - Supports the large 48-port topology (~504 hosts, 23 switches)
      - Pure baseline behavior: no VIPs, no address mutation, no rotation
      - Reactive per-flow forwarding with shortest-path routing
      - Emits benchmark-friendly log tokens:
            TOPO_DISCOVERY_COMPLETE elapsed=X.XXX
            RPPT_MEASURED: key=(...) elapsed_ms=X.XXX
            FLOW_SETUP: key=(...) pathsw=N elapsed_ms=X.XXX
    """

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    REAL_HOST_SUPERNET = "10.0.0.0/8"
    CONTROLLER_DISCOVERY_IP = "10.255.255.254"
    CONTROLLER_DISCOVERY_MAC = "02:00:00:00:00:fe"
    # Keep proactive discovery bounded to the actual real-host region used by the
    # industry topology, mirroring the MTD controller's practical cutoff before
    # VIP space begins.
    REAL_HOST_SCAN_START = "10.0.0.1"
    REAL_HOST_SCAN_END_EXCLUSIVE = "10.0.3.1"

    HOUSEKEEPING_INTERVAL = 15
    DISCOVERY_RETRY_SECONDS = 60
    DISCOVERY_BATCH_SIZE = 128

    FLOW_PRIORITY = 100
    DEFAULT_IDLE_TIMEOUT = 15
    DEFAULT_HARD_TIMEOUT = 0

    EXPECTED_SWITCHES = 23
    EXPECTED_DIRECTED_LINKS = 44
    EXPECTED_HOSTS = 0  # topology completion waits only on switches+links
    REAL_HOST_SCAN_START = "10.0.0.1"
    REAL_HOST_SCAN_END_EXCLUSIVE = "10.0.3.1"
    DISCOVERY_PROGRESS_LOG_EVERY = 30

    def __init__(self, *args, **kwargs):
        super(BaselineReactiveController, self).__init__(*args, **kwargs)
        self.datapaths: Dict[int, object] = {}
        self.mac_to_port: Dict[int, Dict[str, int]] = {}

        # Switch graph: dpid -> neighbor_dpid -> out_port
        self.adj: Dict[int, Dict[int, int]] = {}

        # Host tracking
        self.host_ip_to_mac: Dict[str, str] = {}
        self.host_location: Dict[str, Tuple[int, int, str]] = {}  # ip -> (dpid, port, mac)
        self.mac_location: Dict[str, Tuple[int, int]] = {}        # mac -> (dpid, port)
        self.detected_hosts: Set[str] = set()

        # Discovery timing
        self.discovery_start_time = time()
        self.discovery_end_time: Optional[float] = None
        self.discovery_completed = False
        self.discovery_completion_reason = ""

        # Simple counters for diagnostics
        self.packetin_count = 0
        self.flow_setup_count = 0

        self.real_host_supernet = ipaddress.ip_network(self.REAL_HOST_SUPERNET, strict=False)
        self.controller_discovery_ip = ipaddress.ip_address(self.CONTROLLER_DISCOVERY_IP)
        self.real_host_scan_start_ip = ipaddress.ip_address(self.REAL_HOST_SCAN_START)
        self.real_host_scan_end_exclusive_ip = ipaddress.ip_address(self.REAL_HOST_SCAN_END_EXCLUSIVE)

        # Batched proactive host discovery state
        self._discovery_cursor: int = 1
        self._discovery_batch_size: int = self.DISCOVERY_BATCH_SIZE
        self._last_discovery: Dict[str, float] = {}
        self._candidate_host_count_cache: Optional[int] = None

        self.last_progress_log_at: float = 0.0
        self.discovery_ready_logged: bool = False


    def start(self):
        super(BaselineReactiveController, self).start()
        if getattr(self, "_workers_started", False):
            return
        self._workers_started = True
        self.threads.append(hub.spawn(self._housekeeping_loop))

    def _housekeeping_loop(self):
        while True:
            now = time()
            try:
                self._proactive_discovery(now)
                self._maybe_log_host_discovery_status(now, "Housekeeping")
            except Exception as e:
                self.logger.warning("HOUSEKEEPING_DISCOVERY_FAILED: %s", e)
            hub.sleep(self.HOUSEKEEPING_INTERVAL)

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------
    def _ip_in_real_host_space(self, ip: str) -> bool:
        try:
            addr = ipaddress.ip_address(ip)
        except Exception:
            return False
        if addr.version != 4:
            return False
        if addr not in self.real_host_supernet:
            return False
        if addr == self.controller_discovery_ip:
            return False
        if addr < self.real_host_scan_start_ip:
            return False
        if addr >= self.real_host_scan_end_exclusive_ip:
            return False
        return True

    def _int_to_ip(self, value: int) -> str:
        return str(ipaddress.ip_address(value))

    def _all_candidate_real_ips(self):
        for value in range(int(self.real_host_scan_start_ip), int(self.real_host_scan_end_exclusive_ip)):
            ip = self._int_to_ip(value)
            if self._ip_in_real_host_space(ip):
                yield ip

    def _log_discovery_progress(self, reason: str):
        sw = len(self.datapaths)
        directed_links = sum(len(v) for v in self.adj.values())
        self.logger.info(
            "DISCOVERY_PROGRESS: reason=%s switches=%d/%d directed_links=%d/%d hosts=%d",
            reason,
            sw,
            self.EXPECTED_SWITCHES,
            directed_links,
            self.EXPECTED_DIRECTED_LINKS,
            len(self.detected_hosts),
        )

    def _maybe_log_host_discovery_status(self, now: float, reason: str):
        if (now - self.last_progress_log_at) < self.DISCOVERY_PROGRESS_LOG_EVERY:
            return
        self.last_progress_log_at = now
        learned = len(self.detected_hosts)
        total = self._candidate_host_count()
        pct = (100.0 * learned / total) if total else 0.0
        self.logger.info(
            "HOST_DISCOVERY_PROGRESS: reason=%s learned=%d/%d (%.1f%%) scan_cursor=%d batch=%d",
            reason,
            learned,
            total,
            pct,
            self._discovery_cursor,
            self._discovery_batch_size,
        )
        if learned >= total and total > 0 and not self.discovery_ready_logged:
            self.discovery_ready_logged = True
            self.logger.info(
                "HOST_DISCOVERY_READY: learned=%d/%d all candidate hosts discovered; safe to start large-scale reachability tests",
                learned,
                total,
            )

    def _maybe_complete_discovery(self, reason: str):
        if self.discovery_completed:
            return
        sw = len(self.datapaths)
        directed_links = sum(len(v) for v in self.adj.values())
        have_switches = sw >= self.EXPECTED_SWITCHES
        have_links = directed_links >= self.EXPECTED_DIRECTED_LINKS
        have_hosts = (self.EXPECTED_HOSTS == 0) or (len(self.detected_hosts) >= self.EXPECTED_HOSTS)
        if have_switches and have_links and have_hosts:
            self.discovery_completed = True
            self.discovery_end_time = time()
            elapsed = self.discovery_end_time - self.discovery_start_time
            self.discovery_completion_reason = reason
            self.logger.info(
                "TOPO_DISCOVERY_COMPLETE reason=%s elapsed=%.3f switches=%d directed_links=%d hosts=%d",
                reason,
                elapsed,
                sw,
                directed_links,
                len(self.detected_hosts),
            )

    def _refresh_topology(self, reason: str):
        new_adj: Dict[int, Dict[int, int]] = {}
        try:
            switches = get_switch(self, None)
            links = get_link(self, None)
        except Exception as e:
            self.logger.warning("TOPOLOGY_REFRESH_FAILED: reason=%s err=%s", reason, e)
            return

        for sw in switches:
            new_adj.setdefault(sw.dp.id, {})
        for link in links:
            new_adj.setdefault(link.src.dpid, {})[link.dst.dpid] = link.src.port_no
        self.adj = new_adj
        self._log_discovery_progress(reason)
        self._maybe_complete_discovery(reason)

    # ------------------------------------------------------------------
    # Flow helpers
    # ------------------------------------------------------------------
    def _add_flow(self, dp, priority, match, actions, idle_timeout=0, hard_timeout=0):
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
            datapath=dp,
            priority=priority,
            match=match,
            instructions=inst,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout,
            buffer_id=ofp.OFP_NO_BUFFER,
        )
        dp.send_msg(mod)

    def _send_packet_out(self, msg, dp, in_port, actions):
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=None if msg.buffer_id != ofp.OFP_NO_BUFFER else msg.data,
        )
        dp.send_msg(out)

    def _build_match(self, parser, in_port: int, eth, ip4, pkt):
        if ip4 is None:
            return parser.OFPMatch(in_port=in_port, eth_dst=eth.dst, eth_src=eth.src)

        l4_tcp = pkt.get_protocol(tcp.tcp)
        l4_udp = pkt.get_protocol(udp.udp)
        l4_icmp = pkt.get_protocol(icmp.icmp)

        kwargs = {
            "in_port": in_port,
            "eth_type": ether_types.ETH_TYPE_IP,
            "ipv4_src": ip4.src,
            "ipv4_dst": ip4.dst,
            "ip_proto": ip4.proto,
        }
        if l4_tcp:
            kwargs.update({"tcp_src": l4_tcp.src_port, "tcp_dst": l4_tcp.dst_port})
        elif l4_udp:
            kwargs.update({"udp_src": l4_udp.src_port, "udp_dst": l4_udp.dst_port})
        elif l4_icmp:
            kwargs.update({"icmpv4_type": l4_icmp.type, "icmpv4_code": l4_icmp.code})
        return parser.OFPMatch(**kwargs)

    def _reverse_match(self, parser, out_port: int, eth, ip4, pkt):
        if ip4 is None:
            return parser.OFPMatch(in_port=out_port, eth_dst=eth.src, eth_src=eth.dst)

        l4_tcp = pkt.get_protocol(tcp.tcp)
        l4_udp = pkt.get_protocol(udp.udp)
        l4_icmp = pkt.get_protocol(icmp.icmp)

        kwargs = {
            "in_port": out_port,
            "eth_type": ether_types.ETH_TYPE_IP,
            "ipv4_src": ip4.dst,
            "ipv4_dst": ip4.src,
            "ip_proto": ip4.proto,
        }
        if l4_tcp:
            kwargs.update({"tcp_src": l4_tcp.dst_port, "tcp_dst": l4_tcp.src_port})
        elif l4_udp:
            kwargs.update({"udp_src": l4_udp.dst_port, "udp_dst": l4_udp.src_port})
        elif l4_icmp:
            kwargs.update({"icmpv4_type": 0 if l4_icmp.type == 8 else l4_icmp.type, "icmpv4_code": l4_icmp.code})
        return parser.OFPMatch(**kwargs)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    def _shortest_path(self, src_dpid: int, dst_dpid: int) -> List[int]:
        if src_dpid == dst_dpid:
            return [src_dpid]
        visited = {src_dpid}
        q = deque([(src_dpid, [src_dpid])])
        while q:
            cur, path = q.popleft()
            for nbr in self.adj.get(cur, {}):
                if nbr in visited:
                    continue
                if nbr == dst_dpid:
                    return path + [nbr]
                visited.add(nbr)
                q.append((nbr, path + [nbr]))
        return []

    def _path_ports(self, src_dpid: int, src_host_port: int, dst_dpid: int, dst_host_port: int):
        path = self._shortest_path(src_dpid, dst_dpid)
        if not path:
            return [], {}
        ports: Dict[int, Tuple[int, int]] = {}
        for idx, sw in enumerate(path):
            if len(path) == 1:
                ports[sw] = (src_host_port, dst_host_port)
                continue
            if idx == 0:
                out_port = self.adj[sw][path[idx + 1]]
                ports[sw] = (src_host_port, out_port)
            elif idx == len(path) - 1:
                in_port = self.adj[sw][path[idx - 1]]
                ports[sw] = (in_port, dst_host_port)
            else:
                in_port = self.adj[sw][path[idx - 1]]
                out_port = self.adj[sw][path[idx + 1]]
                ports[sw] = (in_port, out_port)
        return path, ports

    def _is_uplink_port(self, dpid: int, port: int) -> bool:
        return port in self.adj.get(dpid, {}).values()

    def _learn_host(self, pkt, dpid: int, in_port: int, eth) -> Optional[str]:
        # Packets arriving on inter-switch links are not from directly attached hosts;
        # learning from them would corrupt host_location with wrong (dpid, port) pairs.
        if self._is_uplink_port(dpid, in_port):
            return None

        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        real_ip = None
        if arp_pkt:
            real_ip = arp_pkt.src_ip
        elif ip_pkt:
            real_ip = ip_pkt.src
        if not real_ip or not self._ip_in_real_host_space(real_ip):
            return None

        mac = eth.src
        self.host_ip_to_mac[real_ip] = mac
        self.host_location[real_ip] = (dpid, in_port, mac)
        self.mac_location[mac] = (dpid, in_port)
        if real_ip not in self.detected_hosts:
            self.detected_hosts.add(real_ip)
            self.logger.info("HOST_LEARNED: ip=%s mac=%s switch=%016x port=%s", real_ip, mac, dpid, in_port)
            learned = len(self.detected_hosts)
            total = self._candidate_host_count()
            if learned >= total and total > 0 and not self.discovery_ready_logged:
                self.discovery_ready_logged = True
                self.logger.info(
                    "HOST_DISCOVERY_READY: learned=%d/%d all candidate hosts discovered; safe to start large-scale reachability tests",
                    learned,
                    total,
                )
        self._maybe_complete_discovery("HostLearn")
        return real_ip

    def _candidate_host_count(self) -> int:
        if self._candidate_host_count_cache is None:
            self._candidate_host_count_cache = sum(1 for _ in self._all_candidate_real_ips())
        return self._candidate_host_count_cache

    def _proactive_discovery(self, now: float):
        if not self.datapaths:
            return

        candidates = list(self._all_candidate_real_ips())
        if not candidates:
            return

        total = len(candidates)
        start_idx = max(0, self._discovery_cursor - 1)
        end_idx = min(total, start_idx + self._discovery_batch_size)
        batch = candidates[start_idx:end_idx]

        if not batch:
            self._discovery_cursor = 1
            return

        for target_ip in batch:
            if target_ip in self.detected_hosts:
                continue
            if target_ip in self._last_discovery and (now - self._last_discovery[target_ip] < self.DISCOVERY_RETRY_SECONDS):
                continue

            self._last_discovery[target_ip] = now

            for dp in list(self.datapaths.values()):
                try:
                    parser = dp.ofproto_parser
                    ofp = dp.ofproto
                    p = packet.Packet()
                    p.add_protocol(ethernet.ethernet(
                        ethertype=ether_types.ETH_TYPE_ARP,
                        dst='ff:ff:ff:ff:ff:ff',
                        src=self.CONTROLLER_DISCOVERY_MAC,
                    ))
                    p.add_protocol(arp.arp(
                        opcode=arp.ARP_REQUEST,
                        src_mac=self.CONTROLLER_DISCOVERY_MAC,
                        src_ip=self.CONTROLLER_DISCOVERY_IP,
                        dst_mac='00:00:00:00:00:00',
                        dst_ip=target_ip,
                    ))
                    p.serialize()
                    out = parser.OFPPacketOut(
                        datapath=dp,
                        buffer_id=ofp.OFP_NO_BUFFER,
                        in_port=ofp.OFPP_CONTROLLER,
                        actions=[parser.OFPActionOutput(ofp.OFPP_FLOOD)],
                        data=p.data,
                    )
                    dp.send_msg(out)
                except Exception as e:
                    self.logger.debug("DISCOVERY_ARP_FAILED: target=%s dp=%016x err=%s", target_ip, dp.id, e)

        if end_idx >= total:
            self._discovery_cursor = 1
        else:
            self._discovery_cursor = end_idx + 1

    # ------------------------------------------------------------------
    # OpenFlow / topology events
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        self.datapaths[dp.id] = dp
        self.mac_to_port.setdefault(dp.id, {})

        parser = dp.ofproto_parser
        ofp = dp.ofproto
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp, priority=0, match=match, actions=actions)
        self.logger.info("[SW] Switch %016x connected; installed table-miss", dp.id)
        self._refresh_topology("EventOFPSwitchFeatures")

    @set_ev_cls(topo_event.EventSwitchEnter)
    def _switch_enter(self, ev):
        self._refresh_topology("EventSwitchEnter")

    @set_ev_cls(topo_event.EventSwitchLeave)
    def _switch_leave(self, ev):
        self._refresh_topology("EventSwitchLeave")

    @set_ev_cls(topo_event.EventLinkAdd)
    def _link_add(self, ev):
        self._refresh_topology("EventLinkAdd")

    @set_ev_cls(topo_event.EventLinkDelete)
    def _link_delete(self, ev):
        self._refresh_topology("EventLinkDelete")

    # ------------------------------------------------------------------
    # Packet processing
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        rppt_start = time()
        self.packetin_count += 1

        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        dpid = dp.id
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth.src] = in_port
        self.mac_location[eth.src] = (dpid, in_port)
        self._learn_host(pkt, dpid, in_port, eth)

        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self._handle_arp(msg, dp, pkt, eth, arp_pkt, in_port)
            return

        ip4 = pkt.get_protocol(ipv4.ipv4)
        if ip4 is None:
            # Fallback plain L2 forwarding if destination MAC is already known.
            dst_loc = self.mac_location.get(eth.dst)
            if dst_loc is None:
                actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
                self._send_packet_out(msg, dp, in_port, actions)
                return
            self._install_path_and_forward(msg, dp, in_port, eth, None, pkt, dst_loc, rppt_start)
            return

        dst_loc = self._locate_ip_destination(ip4.dst, eth.dst)
        if dst_loc is None:
            # Unknown destination: let ARP solve it or flood conservatively.
            actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
            self._send_packet_out(msg, dp, in_port, actions)
            return

        self._install_path_and_forward(msg, dp, in_port, eth, ip4, pkt, dst_loc, rppt_start)

    def _locate_ip_destination(self, dst_ip: str, dst_mac: str) -> Optional[Tuple[int, int, str]]:
        if dst_ip in self.host_location:
            return self.host_location[dst_ip]
        if dst_mac in self.mac_location:
            dpid, port = self.mac_location[dst_mac]
            return (dpid, port, dst_mac)
        if dst_ip in self.host_ip_to_mac:
            mac = self.host_ip_to_mac[dst_ip]
            dpid, port = self.mac_location.get(mac, (None, None))
            if dpid is not None:
                return (dpid, port, mac)
        return None

    def _handle_arp(self, msg, dp, pkt, eth, arp_pkt, in_port):
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        # Proxy ARP when the target host is known.
        if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip in self.host_location:
            _dst_dpid, _dst_port, dst_mac = self.host_location[arp_pkt.dst_ip]
            p = packet.Packet()
            p.add_protocol(ethernet.ethernet(
                ethertype=ether_types.ETH_TYPE_ARP,
                dst=eth.src,
                src=dst_mac,
            ))
            p.add_protocol(arp.arp(
                opcode=arp.ARP_REPLY,
                src_mac=dst_mac,
                src_ip=arp_pkt.dst_ip,
                dst_mac=arp_pkt.src_mac,
                dst_ip=arp_pkt.src_ip,
            ))
            p.serialize()
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=ofp.OFP_NO_BUFFER,
                in_port=ofp.OFPP_CONTROLLER,
                actions=[parser.OFPActionOutput(in_port)],
                data=p.data,
            )
            dp.send_msg(out)
            return

        # For ARP replies, unicast directly to the requester's known location so the
        # reply doesn't traverse intermediate switches (which would poison host_location).
        if arp_pkt.opcode == arp.ARP_REPLY:
            requester_loc = self.host_location.get(arp_pkt.dst_ip)
            if requester_loc is not None:
                req_dpid, req_port, _ = requester_loc
                req_dp = self.datapaths.get(req_dpid)
                if req_dp is not None:
                    req_parser = req_dp.ofproto_parser
                    req_ofp = req_dp.ofproto
                    req_dp.send_msg(req_parser.OFPPacketOut(
                        datapath=req_dp,
                        buffer_id=req_ofp.OFP_NO_BUFFER,
                        in_port=req_ofp.OFPP_CONTROLLER,
                        actions=[req_parser.OFPActionOutput(req_port)],
                        data=msg.data,
                    ))
                    return

        # Flood for unknown ARP requests and unresolvable ARP replies.
        actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
        self._send_packet_out(msg, dp, in_port, actions)

    def _install_path_and_forward(self, msg, dp, in_port, eth, ip4, pkt, dst_loc, rppt_start):
        dst_dpid, dst_host_port, dst_mac = dst_loc
        src_dpid = dp.id
        path, ports = self._path_ports(src_dpid, in_port, dst_dpid, dst_host_port)
        if not path:
            self.logger.warning(
                "NO_PATH: src_sw=%016x dst_sw=%016x src=%s dst=%s",
                src_dpid,
                dst_dpid,
                getattr(ip4, 'src', eth.src),
                getattr(ip4, 'dst', eth.dst),
            )
            return

        total_mods = 0
        for sw in path:
            sw_dp = self.datapaths.get(sw)
            if sw_dp is None:
                continue
            parser = sw_dp.ofproto_parser
            sw_in, sw_out = ports[sw]
            actions = [parser.OFPActionSetField(eth_dst=dst_mac), parser.OFPActionOutput(sw_out)]
            match = self._build_match(parser, sw_in, eth, ip4, pkt)
            self._add_flow(
                sw_dp,
                priority=self.FLOW_PRIORITY,
                match=match,
                actions=actions,
                idle_timeout=self.DEFAULT_IDLE_TIMEOUT,
                hard_timeout=self.DEFAULT_HARD_TIMEOUT,
            )
            total_mods += 1

        # Also install reverse path when both endpoints are known.
        src_mac = eth.src
        src_loc = self.mac_location.get(src_mac)
        if src_loc is not None:
            src_host_dpid, src_host_port = src_loc
            rev_path, rev_ports = self._path_ports(dst_dpid, dst_host_port, src_host_dpid, src_host_port)
            for sw in rev_path:
                sw_dp = self.datapaths.get(sw)
                if sw_dp is None:
                    continue
                parser = sw_dp.ofproto_parser
                sw_in, sw_out = rev_ports[sw]
                actions = [parser.OFPActionSetField(eth_dst=src_mac), parser.OFPActionOutput(sw_out)]
                match = self._reverse_match(parser, sw_in, eth, ip4, pkt)
                self._add_flow(
                    sw_dp,
                    priority=self.FLOW_PRIORITY,
                    match=match,
                    actions=actions,
                    idle_timeout=self.DEFAULT_IDLE_TIMEOUT,
                    hard_timeout=self.DEFAULT_HARD_TIMEOUT,
                )
                total_mods += 1

        first_out = ports[path[0]][1]
        parser = dp.ofproto_parser
        actions = [parser.OFPActionSetField(eth_dst=dst_mac), parser.OFPActionOutput(first_out)]
        self._send_packet_out(msg, dp, in_port, actions)

        elapsed_ms = (time() - rppt_start) * 1000.0
        key = self._format_key(ip4, pkt)
        self.flow_setup_count += 1
        self.logger.info("RPPT_MEASURED: key=%s elapsed_ms=%.3f", key, elapsed_ms)
        self.logger.info(
            "FLOW_SETUP: key=%s pathsw=%d flowmods=%d elapsed_ms=%.3f",
            key,
            len(path),
            total_mods,
            elapsed_ms,
        )

    def _format_key(self, ip4, pkt) -> str:
        if ip4 is None:
            return "(non-ip)"
        t = pkt.get_protocol(tcp.tcp)
        if t:
            return "(%s,%s,TCP,%s,%s)" % (ip4.src, ip4.dst, t.src_port, t.dst_port)
        u = pkt.get_protocol(udp.udp)
        if u:
            return "(%s,%s,UDP,%s,%s)" % (ip4.src, ip4.dst, u.src_port, u.dst_port)
        i = pkt.get_protocol(icmp.icmp)
        if i:
            return "(%s,%s,ICMP,%s,%s)" % (ip4.src, ip4.dst, i.type, i.code)
        return "(%s,%s,IP,%s)" % (ip4.src, ip4.dst, ip4.proto)
