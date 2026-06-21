import os
import json
import socket
import ipaddress
import subprocess
from time import time
from typing import Dict, List, Optional, Set, Tuple

from ryu.base import app_manager
from ryu.controller import event, ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import arp, ethernet, icmp, ipv4, packet, tcp, udp
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_link, get_switch


class EventMessage(event.EventBase):
    def __init__(self, message: str):
        super(EventMessage, self).__init__()


class MovingTargetDefenseDNS(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _EVENTS = [EventMessage]

    NUM_VIPS = 6000
    HOUSEKEEPING_INTERVAL = 15
    ROTATE_INTERVAL = 60
    ROTATION_BATCH_SIZE = 20       # rotate this many hosts, then pause
    ROTATION_BATCH_DELAY = 0.15    # seconds between batches — spreads PacketIn storm over ~4s
    TCP_SYN_SEEN_TIMEOUT = 15
    TCP_ESTABLISHED_TIMEOUT = 15
    TCP_CLOSING_TIMEOUT = 8
    UDP_ACTIVE_TIMEOUT = 30
    VIP_INACTIVITY_RECLAIM = 5
    VIP_QUARANTINE_SECONDS = 30
    FLOW_STATS_POLL = 20           # seconds between OVS flow-stats polls (idle-VIP reclaim)

    # ---------------- address plan ----------------
    # Real hosts live in the large 10.0.0.0/8 fabric from test_topo.py.
    REAL_HOST_SUPERNET = "10.0.0.0/8"
    CONTROLLER_DISCOVERY_IP = "10.255.255.254"
    CONTROLLER_DISCOVERY_MAC = "02:00:00:00:00:fe"

    # In the large topology, real hosts occupy early 10.0.x.x space and VIPs start later.
    VIP_POOL_START = "10.0.3.1"

    FLOW_PRIORITY_VIP = 100
    COOKIE_BASE = 0xA000_0000_0000_0000
    COOKIE_VIP_MASK = 0xFFFF_FFFF

    # 24-port medium topology (industrytp.py): 1 core + 1 agg + 9 edge = 11 switches.
    # Links: 1 agg-core + 9 edge-agg = 10 undirected = 20 directed.
    EXPECTED_SWITCHES = 11
    EXPECTED_DIRECTED_LINKS = 20
    EXPECTED_HOSTS = 0  # 0 = do not wait for host discovery

    # Path written by industrytp.py; maps real_ip -> {pid, iface, mac}
    NS_MAP_FILE = "/tmp/mininet_ns_map.json"

    VIP_STATE_PRIMARY = "PRIMARY"
    VIP_STATE_GRACE = "GRACE"
    SESSION_TCP_SYN_SEEN = "SYN_SEEN"
    SESSION_TCP_ESTABLISHED = "ESTABLISHED"
    SESSION_TCP_CLOSING = "CLOSING"
    SESSION_UDP_ACTIVE = "ACTIVE"

    def __init__(self, *args, **kwargs):
        super(MovingTargetDefenseDNS, self).__init__(*args, **kwargs)

        # Network topology tracking
        self.mac_to_port: Dict[int, Dict[str, int]] = {}  # dpid -> {mac: port}
        self.datapaths: Set["ryu.controller.controller.Datapath"] = set()
        self.switch_link_ports: Dict[int, Set[int]] = {}  # dpid -> set of ports connecting to other switches
        # Shortest-path routing between switches, from LLDP topology:
        #   route_next_port[src_dpid][dst_dpid] = local out-port toward dst_dpid.
        # Used to forward VIP-tagged traffic across transit switches WITHOUT
        # flooding (transit switches never learn synthetic VIP MACs).
        self.route_next_port: Dict[int, Dict[int, int]] = {}
        self._port_peer: Dict[int, Dict[int, int]] = {}  # dpid -> {out_port: neighbor_dpid}
        
        # Host discovery and mapping
        self.detected_hosts: Set[str] = set()  # Set of discovered real host IPs
        self.host_ip_to_mac: Dict[str, str] = {}  # Real host IP -> MAC address
        self.host_location: Dict[str, Tuple[int, int, str]] = {}  # real_ip -> (dpid, port, mac)
        
        # VIP assignment and state
        self.primary_vip: Dict[str, str] = {}  # Real host IP -> Primary VIP assigned to that host
        self.vip_owner: Dict[str, str] = {}  # VIP -> Real host IP (reverse mapping)
        self.vip_state: Dict[str, str] = {}  # VIP -> State (PRIMARY, GRACE)
        self.vip_mac_map: Dict[str, str] = {}  # VIP -> Generated MAC address for that VIP
        self.vip_created_at: Dict[str, float] = {}  # VIP -> Timestamp when VIP was created/assigned
        self.host_vip_pools: Dict[str, Set[str]] = {}  # Real host IP -> Set of all VIPs assigned to that host
        
        # Activity tracking
        self.vip_flow_refs: Dict[str, int] = {}  # VIP -> Number of installed dataplane flows still alive
        # VIP -> set of DISTINCT flow identities (dpid + match signature). Used to
        # derive vip_flow_refs idempotently: re-installing the same match (which
        # OVS treats as a MODIFY and never sends a FlowRemoved for) must NOT bump
        # the ref count, or it drifts above 0 forever and the VIP is never reclaimed.
        self.vip_flow_keys: Dict[str, Set[Tuple]] = {}
        self.vip_session_refs: Dict[str, int] = {}  # VIP -> Active controller-side sessions pinned to VIP
        self.vip_last_seen: Dict[str, float] = {}  # VIP -> Last observed session activity
        self.quarantine_until: Dict[str, float] = {}  # VIP -> Earliest timestamp eligible for reuse
        self.vip_delete_requested_at: Dict[str, float] = {}  # VIP -> Last cookie delete request time

        # L4 session handling for TCP/UDP NAT consistency
        self.session_table: Dict[Tuple[str, str, int, int, int], Dict[str, object]] = {}
        
        # VIP resource pool
        self.Resources: List[str] = self._generate_vips(self.VIP_POOL_START, self.NUM_VIPS)

        # Address-plan helpers
        self.real_host_supernet = ipaddress.ip_network(self.REAL_HOST_SUPERNET, strict=False)
        self.vip_pool_start_ip = ipaddress.ip_address(self.VIP_POOL_START)
        self.controller_discovery_ip = ipaddress.ip_address(self.CONTROLLER_DISCOVERY_IP)

        # Flow placement tracking: VIP -> datapaths known to hold VIP-tagged flows
        self.vip_rule_locations: Dict[str, Set[int]] = {}

        # Batched proactive discovery state
        self._discovery_cursor: int = 1
        self._discovery_batch_size: int = 128
        self._last_discovery: Dict[str, float] = {}

        # Topology discovery timing (switches + links only)
        self.discovery_start_time = time()
        self.discovery_end_time: Optional[float] = None
        self.discovery_completed = False
        self.discovery_completion_reason = ""

        # Option 1: host namespace map for VIP push via nsenter
        self._ns_map: Dict[str, dict] = {}
        self._ns_map_bootstrapped: bool = False
        # Outstanding non-blocking nsenter (VIP push/remove) child processes.
        self._push_procs: List["subprocess.Popen"] = []
        # Flow-stats-based idle reclaim (FlowRemoved is unreliable under load):
        # per-VIP cumulative packet counts from two consecutive polls.
        self._vip_pkts_cur: Dict[str, int] = {}
        self._vip_pkts_prev: Dict[str, int] = {}
        self._stats_poll_n: int = 0

        # Remove any stale VIP mapping left by a previous controller/run so the
        # benchmark (which reads /tmp/mtd_vip_mapping.json) can never target
        # VIPs that this controller did not assign.  This controller rewrites
        # the file as it binds VIPs; deleting it here guarantees it only ever
        # reflects the current run.
        try:
            os.remove("/tmp/mtd_vip_mapping.json")
            self.logger.info("STARTUP: removed stale /tmp/mtd_vip_mapping.json")
        except OSError:
            pass

        # Override hardcoded topology expectations from the file the topology
        # script writes (campus_topo.py / industry_topo.py), so discovery
        # completes for any topology size without editing constants.  The file
        # may not exist yet at startup (controller launched before the topology
        # finishes), so this is retried during discovery until it loads once.
        self._expectations_loaded = False
        self._load_topology_expectations()

    def _load_topology_expectations(self) -> bool:
        """Load expected switch/link counts from /tmp/mtd_topology_expectations.json.

        Returns True once the file is successfully read.  Falls back to the
        hardcoded EXPECTED_* class attributes while the file is absent, so the
        controller adapts to any topology size/shape without code changes.
        """
        if self._expectations_loaded:
            return True
        path = "/tmp/mtd_topology_expectations.json"
        try:
            with open(path) as f:
                exp = json.load(f)
            self.EXPECTED_SWITCHES = int(exp.get("expected_switches", self.EXPECTED_SWITCHES))
            self.EXPECTED_DIRECTED_LINKS = int(exp.get("expected_directed_links", self.EXPECTED_DIRECTED_LINKS))
            self.EXPECTED_HOSTS = int(exp.get("expected_hosts", self.EXPECTED_HOSTS))
            self._expectations_loaded = True
            self.logger.info(
                "TOPO_EXPECT: loaded from %s — switches=%d directed_links=%d hosts=%d",
                path, self.EXPECTED_SWITCHES, self.EXPECTED_DIRECTED_LINKS, self.EXPECTED_HOSTS,
            )
            return True
        except Exception:
            return False

    # ---------------- lifecycle ----------------

    def start(self):
        super(MovingTargetDefenseDNS, self).start()
        if getattr(self, "_workers_started", False):
            self.logger.warning("START: worker threads already started, skipping duplicate spawn")
            return
        self._workers_started = True
        self.threads.append(hub.spawn(self._ticker))
        self.threads.append(hub.spawn(self._rotation_loop))
        self.threads.append(hub.spawn(self._flow_stats_loop))

    def _ticker(self):
        while True:
            self.send_event_to_observers(EventMessage("TICK"))
            hub.sleep(self.HOUSEKEEPING_INTERVAL)

    @set_ev_cls(EventMessage)
    def _housekeeping(self, ev):
        """Periodic housekeeping tasks."""
        now = time()
        # Option 1: bootstrap host VIPs from ns_map (replaces proactive ARP discovery)
        self._bootstrap_from_ns_map()
        # Proactive host discovery (no-op once all hosts are in detected_hosts)
        self._proactive_discovery(now)

        # Refresh the switch-to-switch routing table.  Discovery can complete
        # the moment the link count crosses the threshold, before BOTH
        # directions of every link are in get_link — which builds an asymmetric
        # table (some src->dst routes missing, causing transit flooding one way).
        # Rebuilding each tick self-heals once all links are stably present.
        if self.discovery_completed:
            try:
                links = get_link(self, None)
                # Only rebuild from a sufficiently complete link set. A degraded
                # get_link (LLDP loss under load) must NOT shrink a good table —
                # that creates a flood→LLDP-loss→worse-routing feedback loop.
                if links and len(links) >= self.EXPECTED_DIRECTED_LINKS:
                    self._build_routing_table(links)
            except Exception as e:
                self.logger.debug("ROUTE: periodic rebuild skipped: %s", e)

        # Move VIPs from quarantine back to resources when cooldown expires
        for vip, ready_at in list(self.quarantine_until.items()):
            if ready_at <= now:
                self.quarantine_until.pop(vip, None)
                if vip not in self.Resources:
                    self.Resources.append(vip)
                    self.logger.info("QUARANTINE: VIP %s cooldown expired, returned to pool", vip)
        
        # Fallback for switches that do not reliably emit FlowRemoved on delete-by-cookie.
        # If we already requested delete and the VIP is GRACE with no controller sessions,
        # avoid stale flow refs pinning the VIP forever.
        for vip, delete_ts in list(self.vip_delete_requested_at.items()):
            if self.vip_state.get(vip) != self.VIP_STATE_GRACE:
                continue
            if self.vip_session_refs.get(vip, 0) > 0:
                continue
            if self.vip_flow_refs.get(vip, 0) <= 0:
                self.vip_delete_requested_at.pop(vip, None)
                continue
            if (now - delete_ts) >= self.VIP_INACTIVITY_RECLAIM:
                self.logger.warning(
                    "FLOW_DELETE_FALLBACK: VIP %s forcing flow refs %d->0 after delete request timeout",
                    vip,
                    self.vip_flow_refs.get(vip, 0),
                )
                self.vip_flow_refs[vip] = 0
                self.vip_flow_keys.pop(vip, None)
                # CRITICAL: Touch VIP when forcing flow_refs to 0 to start inactivity timer
                # This ensures destination VIP gets reclaimed after 5s inactivity
                self._touch_vip(vip, now)
                self.vip_delete_requested_at.pop(vip, None)

        # Handle VIPs in GRACE state
        # Use flow_refs to determine activity: if flow_refs > 0, VIP is active; if flow_refs = 0, VIP is idle
        # IMPORTANT: Only process GRACE VIPs - PRIMARY VIPs should never be reclaimed
        for vip in list(self.vip_state.keys()):
            if self.vip_state.get(vip) != self.VIP_STATE_GRACE:
                continue
            
            flow_refs = self.vip_flow_refs.get(vip, 0)
            session_refs = self.vip_session_refs.get(vip, 0)
            last_seen = self.vip_last_seen.get(vip, 0)
            inactive_for = now - last_seen

            # For UDP sessions: If server VIP has no flows but client VIP (forward flow) is still active,
            # keep server VIP active. This handles cases where reverse flow expires but forward flow continues.
            has_active_udp_forward_flow = False
            if flow_refs == 0:
                for sess in self.session_table.values():
                    if (sess.get("server_reply_vip") == vip and 
                        sess.get("proto") == socket.IPPROTO_UDP and
                        sess.get("expires_at", 0) > now):
                        client_vip = sess.get("client_src_vip")
                        if client_vip and self.vip_flow_refs.get(client_vip, 0) > 0:
                            has_active_udp_forward_flow = True
                            self.logger.debug(
                                "GRACE: VIP %s (server) has active UDP forward flow (client VIP %s has %d flows), keeping active",
                                vip, client_vip, self.vip_flow_refs.get(client_vip, 0)
                            )
                            break

            if flow_refs <= 0 and session_refs <= 0 and not has_active_udp_forward_flow and inactive_for >= self.VIP_INACTIVITY_RECLAIM:
                self.logger.info("RECLAIM: VIP %s idle for %.1fs, reclaiming", vip, inactive_for)
                self._delete_flows_by_cookie(vip)
                self._reclaim_vip(vip)
            else:
                self.logger.debug(
                    "GRACE: VIP %s keep (flow_refs=%d session_refs=%d inactive_for=%.1fs has_udp_forward=%s)",
                    vip,
                    flow_refs,
                    session_refs,
                    inactive_for,
                    has_active_udp_forward_flow,
                )

        self._expire_sessions(now)
        
        # Log VIP pools
        self._log_vip_pools(now)

    # ---------------- utils ----------------

    def _generate_vips(self, start_ip: str, count: int) -> List[str]:
        base = list(map(int, start_ip.split('.')))
        out: List[str] = []
        for _ in range(count):
            out.append('.'.join(map(str, base)))
            base[3] += 1
            for i in (3, 2, 1):
                if base[i] > 255:
                    base[i] = 0
                    base[i - 1] += 1
        return out

    def _generate_vip_mac(self, vip_ip: str) -> str:
        o = [int(x) for x in vip_ip.split('.')]
        return "02:%02x:%02x:%02x:%02x:%02x" % (
            (o[0] ^ 0xAA) & 0xFF,
            (o[1] ^ 0x55) & 0xFF,
            o[2],
            o[3],
            (o[2] ^ o[3]) & 0xFF,
        )

    def _ip_to_int(self, ip: str) -> int:
        p = ip.split('.')
        return (int(p[0]) << 24) + (int(p[1]) << 16) + (int(p[2]) << 8) + int(p[3])

    def _vip_cookie(self, vip: str) -> int:
        return self.COOKIE_BASE | (self._ip_to_int(vip) & self.COOKIE_VIP_MASK)

    def _cookie_vip_ip(self, cookie: int) -> str:
        vip_int = cookie & self.COOKIE_VIP_MASK
        return ".".join([
            str((vip_int >> 24) & 0xFF),
            str((vip_int >> 16) & 0xFF),
            str((vip_int >> 8) & 0xFF),
            str(vip_int & 0xFF),
        ])

    def _int_to_ip(self, value: int) -> str:
        return str(ipaddress.ip_address(value))

    def _ip_in_real_host_space(self, ip: str) -> bool:
        """Return True for managed real hosts in the 10.0.0.0/8 fabric."""
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
        # Treat addresses at/after the VIP pool start as VIP space, not real-host space.
        if addr >= self.vip_pool_start_ip:
            return False
        return True

    def _all_candidate_real_ips(self):
        """Enumerate candidate real-host addresses below VIP_POOL_START."""
        start = int(ipaddress.ip_address("10.0.0.1"))
        end = int(self.vip_pool_start_ip) - 1
        for value in range(start, end + 1):
            ip = self._int_to_ip(value)
            if self._ip_in_real_host_space(ip):
                yield ip

    def _remember_host_location(self, real_ip: str, dpid: int, port: int, mac: Optional[str]):
        if not real_ip or not mac:
            return
        # If there is already a location recorded, only overwrite if the new observation
        # comes from a port with fewer distinct MACs (i.e., more likely a leaf port).
        # This prevents transit-switch re-learning from clobbering the correct edge entry.
        existing = self.host_location.get(real_ip)
        if existing:
            ex_dpid, ex_port, _ = existing
            cur_map_new = self.mac_to_port.get(dpid, {})
            cur_map_ex = self.mac_to_port.get(ex_dpid, {})
            new_count = sum(1 for p in cur_map_new.values() if p == port)
            ex_count = sum(1 for p in cur_map_ex.values() if p == ex_port)
            if new_count >= ex_count:
                return  # keep the existing (better or equally good) location
        self.host_location[real_ip] = (dpid, port, mac)
        self.host_ip_to_mac[real_ip] = mac

    def _delete_flows_by_cookie(self, vip: str):
        """Delete all flows for a VIP, targeting only switches known to hold them."""
        cookie = self._vip_cookie(vip)
        cookie_mask = 0xFFFFFFFFFFFFFFFF

        self.vip_delete_requested_at[vip] = time()

        target_dpids = set(self.vip_rule_locations.get(vip, set()))
        if target_dpids:
            target_dps = [dp for dp in self.datapaths if dp.id in target_dpids]
        else:
            # Fallback when we do not yet have placement knowledge.
            target_dps = list(self.datapaths)

        for dp in target_dps:
            try:
                parser = dp.ofproto_parser
                ofp = dp.ofproto
                mod = parser.OFPFlowMod(
                    datapath=dp,
                    table_id=ofp.OFPTT_ALL,
                    command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY,
                    out_group=ofp.OFPG_ANY,
                    cookie=cookie,
                    cookie_mask=cookie_mask,
                )
                dp.send_msg(mod)
                self.logger.info(
                    "FLOW_DELETE: vip=%s dp=%016x cookie=0x%016x mask=0x%016x",
                    vip, dp.id, cookie, cookie_mask,
                )
            except Exception as e:
                self.logger.warning("FLOW_DELETE: Failed on dp=%016x for VIP %s: %s", dp.id, vip, e)
        # Don't reset flow_refs here - let flow removal events decrement it naturally
        # This prevents race conditions where flows are deleted but flow_refs is reset before flows actually expire

    def _add_flow(self, dp, priority, match, actions, table_id=0, idle_timeout=0, hard_timeout=0, buffer_id=None, cookie=0):
        """Install a flow rule on the switch."""
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        if buffer_id is None:
            buffer_id = ofp.OFP_NO_BUFFER
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
            datapath=dp,
            table_id=table_id,
            priority=priority,
            match=match,
            instructions=inst,
            cookie=cookie,
            flags=ofp.OFPFF_SEND_FLOW_REM,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout,
            buffer_id=buffer_id,
        )
        dp.send_msg(mod)
        if cookie & self.COOKIE_BASE:
            vip = self._cookie_vip_ip(cookie)
            keys = self.vip_flow_keys.setdefault(vip, set())
            sig = self._flow_sig(dp.id, match)
            old_refs = len(keys)
            is_new = sig not in keys
            keys.add(sig)
            self.vip_flow_refs[vip] = len(keys)
            self.vip_rule_locations.setdefault(vip, set()).add(dp.id)
            if is_new:
                self.logger.info(
                    "FLOW_ADD: vip=%s refs=%d->%d dp=%016x idle_timeout=%s match=%s",
                    vip, old_refs, len(keys), dp.id, idle_timeout, match,
                )
            else:
                # Duplicate install of an existing flow (MODIFY) — ref count unchanged.
                self.logger.debug(
                    "FLOW_ADD_DUP: vip=%s refs=%d (re-install) dp=%016x match=%s",
                    vip, len(keys), dp.id, match,
                )

    def _flow_sig(self, dpid: int, match) -> Tuple:
        """Stable identity for a flow = (dpid, sorted OXM match fields).

        Computed the same way for installs and FlowRemoved echoes so the two
        correlate, letting vip_flow_refs be derived from a set of live flows.
        """
        try:
            oxm = match.to_jsondict()["OFPMatch"]["oxm_fields"]
            fields = tuple(sorted(
                (t["OXMTlv"]["field"], str(t["OXMTlv"].get("value")), str(t["OXMTlv"].get("mask")))
                for t in oxm
            ))
        except Exception:
            fields = (str(match),)
        return (dpid, fields)

    @set_ev_cls(ofp_event.EventOFPFlowRemoved, MAIN_DISPATCHER)
    def _flow_removed(self, ev):
        msg = ev.msg
        cookie = msg.cookie
        if not (cookie & self.COOKIE_BASE):
            return

        vip = self._cookie_vip_ip(cookie)
        if vip not in self.vip_owner:
            return

        old_refs = self.vip_flow_refs.get(vip, 0)
        keys = self.vip_flow_keys.get(vip)
        if keys is not None:
            keys.discard(self._flow_sig(ev.msg.datapath.id, msg.match))
            self.vip_flow_refs[vip] = len(keys)
        else:
            self.vip_flow_refs[vip] = max(0, old_refs - 1)
        new_refs = self.vip_flow_refs.get(vip, 0)
        if new_refs <= 0:
            self.vip_delete_requested_at.pop(vip, None)
            self.vip_rule_locations.pop(vip, None)
            # CRITICAL: When last flow expires, touch VIP to start inactivity timer
            # This ensures destination VIP gets reclaimed after 5s inactivity
            self._touch_vip(vip)

        self.logger.debug("FLOW_REMOVED: VIP %s flow expired (refs: %d -> %d, state=%s)", 
                         vip, old_refs, new_refs, self.vip_state.get(vip, "UNKNOWN"))
        
        # For UDP sessions: If both client and server VIPs have flow_refs == 0,
        # remove the session from the table to prevent reinstallation when late packets arrive
        if new_refs == 0:
            for key, sess in list(self.session_table.items()):
                if sess.get("proto") != socket.IPPROTO_UDP:
                    continue
                client_vip = str(sess.get("client_src_vip", ""))
                server_reply_vip = str(sess.get("server_reply_vip", ""))
                # If this VIP matches either client or server VIP, check if both are idle
                if vip == client_vip or vip == server_reply_vip:
                    client_refs = self.vip_flow_refs.get(client_vip, 0)
                    server_refs = self.vip_flow_refs.get(server_reply_vip, 0)
                    if client_refs == 0 and server_refs == 0:
                        self.logger.debug(
                            "FLOW_REMOVED: Removing UDP session (both VIPs idle: client=%s server=%s)",
                            client_vip, server_reply_vip
                        )
                        # Unpin VIPs before removing session
                        self._unpin_vip_session(client_vip)
                        self._unpin_vip_session(server_reply_vip)
                        self.session_table.pop(key, None)
                        # The partner VIP (not the one whose flow just expired) just got
                        # unpinned. If it's a GRACE VIP that's now fully idle, reclaim it
                        # immediately rather than waiting for the next housekeeping cycle.
                        partner = server_reply_vip if vip == client_vip else client_vip
                        now = time()
                        if (partner != vip
                                and self.vip_state.get(partner) == self.VIP_STATE_GRACE
                                and self.vip_flow_refs.get(partner, 0) == 0
                                and self.vip_session_refs.get(partner, 0) == 0
                                and (now - self.vip_last_seen.get(partner, 0)) >= self.VIP_INACTIVITY_RECLAIM):
                            self.logger.info("FLOW_REMOVED: VIP %s (GRACE partner) idle after session end, reclaiming", partner)
                            self._delete_flows_by_cookie(partner)
                            self._reclaim_vip(partner)
                        break
        
        # If GRACE VIP has no flows/sessions left, reclaim immediately
        if (
            self.vip_state.get(vip) == self.VIP_STATE_GRACE
            and new_refs == 0
            and self.vip_session_refs.get(vip, 0) == 0
            and (time() - self.vip_last_seen.get(vip, 0)) >= self.VIP_INACTIVITY_RECLAIM
        ):
            self.logger.info("FLOW_REMOVED: VIP %s (GRACE) now idle, reclaiming", vip)
            self._delete_flows_by_cookie(vip)
            self._reclaim_vip(vip)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply(self, ev):
        """Accumulate per-VIP cumulative packet counts from a flow-stats poll."""
        for stat in ev.msg.body:
            c = stat.cookie
            if not (c & self.COOKIE_BASE):
                continue
            vip = self._cookie_vip_ip(c)
            self._vip_pkts_cur[vip] = self._vip_pkts_cur.get(vip, 0) + stat.packet_count

    def _flow_stats_loop(self):
        """Reclaim GRACE VIPs that OVS confirms are idle.

        FlowRemoved is effectively never delivered under load, so flow_refs for
        UDP VIPs (no explicit teardown) stays stuck > 0 and the VIP is never
        reclaimed (symptom: hours-old "GRACE/ACTIVE" VIPs). Instead we poll OVS
        flow stats: a GRACE VIP whose flows show NO new packets across a poll
        interval — or that OVS has no flows for at all — is genuinely idle and
        safe to reclaim. PRIMARY VIPs and VIPs whose packet count is still
        growing (active streams) are never touched, so this can't break a live
        session the way a control-plane timer (vip_last_seen) would.
        """
        while True:
            hub.sleep(self.FLOW_STATS_POLL)
            try:
                if self._stats_poll_n >= 2:
                    self._reclaim_idle_grace_vips()
            except Exception as e:
                self.logger.debug("STATS: idle sweep error: %s", e)
            # Rotate snapshots; replies to the request below accumulate in _cur.
            self._vip_pkts_prev = self._vip_pkts_cur
            self._vip_pkts_cur = {}
            self._stats_poll_n += 1
            for dp in list(self.datapaths):
                try:
                    parser = dp.ofproto_parser
                    dp.send_msg(parser.OFPFlowStatsRequest(dp))
                except Exception:
                    pass

    def _reclaim_idle_grace_vips(self):
        """Reclaim GRACE VIPs confirmed idle by two consecutive flow-stats polls."""
        reclaimed = 0
        for vip in list(self.vip_state.keys()):
            if self.vip_state.get(vip) != self.VIP_STATE_GRACE:
                continue
            if self.vip_session_refs.get(vip, 0) > 0:
                continue                       # still pinned by a live session
            cur = self._vip_pkts_cur.get(vip)
            prev = self._vip_pkts_prev.get(vip)
            no_flows = (cur is None and prev is None)                       # OVS has no flows
            flat = (cur is not None and prev is not None and cur == prev)   # zero new packets
            if not (no_flows or flat):
                continue                       # active, or not enough data yet -> keep
            self.logger.info(
                "STATS_RECLAIM: GRACE VIP %s idle (%s) -> reclaiming",
                vip, "no flows in OVS" if no_flows else ("packets flat at %s" % cur),
            )
            self._delete_flows_by_cookie(vip)
            self.vip_flow_keys.pop(vip, None)
            self.vip_flow_refs[vip] = 0
            self._reclaim_vip(vip)
            reclaimed += 1
        if reclaimed:
            self.logger.info("STATS_RECLAIM: reclaimed %d idle GRACE VIP(s) this sweep", reclaimed)

    def _take_resource_vip(self) -> Optional[str]:
        """Take a VIP from the resource pool."""
        if self.Resources:
            return self.Resources.pop(0)
        return None

    def _touch_vip(self, vip: str, now: Optional[float] = None):
        if not vip:
            return
        self.vip_last_seen[vip] = now if now is not None else time()

    def _bind_primary_vip(self, host_ip: str, vip: str, now: float):
        """Bind a VIP as the primary VIP for a host."""
        self.primary_vip[host_ip] = vip
        self.vip_owner[vip] = host_ip
        self.vip_state[vip] = self.VIP_STATE_PRIMARY
        self.vip_mac_map[vip] = self._generate_vip_mac(vip)
        self.vip_created_at[vip] = now
        self._touch_vip(vip, now)
        self.host_vip_pools.setdefault(host_ip, set()).add(vip)
        self.logger.info("BIND: host=%s vip=%s", host_ip, vip)
        # Option 1: push VIP onto host's network interface
        self._push_vip_to_host(host_ip, vip)
        # Update DNS mapping file
        self._update_dns_mapping()

    # -------- Option 1: nsenter-based VIP push --------

    def _load_ns_map(self):
        """Load the host namespace map written by industrytp.py."""
        try:
            with open(self.NS_MAP_FILE) as f:
                self._ns_map = json.load(f)
            for real_ip, entry in self._ns_map.items():
                if 'mac' in entry:
                    self.host_ip_to_mac[real_ip] = entry['mac']
            self.logger.info("NS_MAP: loaded %d entries from %s", len(self._ns_map), self.NS_MAP_FILE)
        except Exception as e:
            self.logger.warning("NS_MAP: could not load %s: %s", self.NS_MAP_FILE, e)

    def _spawn_nsenter(self, cmd: str):
        """Run an nsenter command NON-BLOCKING (fire-and-forget).

        CRITICAL: rotation calls this ~once per host every ROTATE_INTERVAL. Using
        the old blocking os.system() froze the single-threaded controller for
        seconds (100 nsenter calls), dropping PacketIns and causing periodic
        UDP/ICMP loss at every rotation. Popen forks+returns immediately; we reap
        finished children opportunistically to avoid zombies.
        """
        try:
            p = subprocess.Popen(cmd, shell=True,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._push_procs.append(p)
        except Exception as e:
            self.logger.warning("VIP_PUSH: spawn failed: %s", e)
        # Reap any finished children (poll() returns exit code and reaps zombie).
        if len(self._push_procs) > 32:
            self._push_procs = [q for q in self._push_procs if q.poll() is None]

    def _push_vip_to_host(self, real_ip: str, vip: str):
        """Add a VIP to the host's network interface via nsenter (non-blocking)."""
        if not self._ns_map:
            self._load_ns_map()
        entry = self._ns_map.get(real_ip)
        if not entry:
            self.logger.warning("VIP_PUSH: no ns_map entry for %s", real_ip)
            return
        pid, iface = entry['pid'], entry['iface']
        self._spawn_nsenter("nsenter -t %d -n ip addr add %s/8 dev %s 2>/dev/null" % (pid, vip, iface))
        self.logger.info("VIP_PUSH: add vip=%s host=%s pid=%d iface=%s (async)",
                         vip, real_ip, pid, iface)

    def _remove_vip_from_host(self, real_ip: str, vip: str):
        """Remove a VIP from the host's network interface via nsenter (non-blocking)."""
        entry = self._ns_map.get(real_ip)
        if not entry:
            return
        pid, iface = entry['pid'], entry['iface']
        self._spawn_nsenter("nsenter -t %d -n ip addr del %s/8 dev %s 2>/dev/null" % (pid, vip, iface))
        self.logger.info("VIP_REMOVE: del vip=%s host=%s pid=%d iface=%s (async)",
                         vip, real_ip, pid, iface)

    def _bootstrap_from_ns_map(self):
        """Pre-populate host detection and assign initial VIPs from ns_map.

        Called from housekeeping once switches are connected.  In Option 1,
        proactive ARP discovery is a no-op (real IPs are gone), so the ns_map
        is the sole source of host information at startup.
        """
        if self._ns_map_bootstrapped:
            return
        if not self.datapaths:
            return
        if not self._ns_map:
            self._load_ns_map()
        if not self._ns_map:
            return  # file not written yet; retry on next housekeeping tick
        now = time()
        count = 0
        for real_ip, entry in self._ns_map.items():
            if real_ip in self.detected_hosts:
                continue
            self.detected_hosts.add(real_ip)
            self.host_vip_pools.setdefault(real_ip, set())
            if 'mac' in entry:
                self.host_ip_to_mac[real_ip] = entry['mac']
            new_vip = self._take_resource_vip()
            if new_vip:
                self._bind_primary_vip(real_ip, new_vip, now)
                count += 1
        self._ns_map_bootstrapped = True
        self.logger.info("BOOTSTRAP: done — %d hosts pre-populated from ns_map", count)

    def _pin_vip_session(self, vip: str):
        if not vip:
            return
        self.vip_session_refs[vip] = self.vip_session_refs.get(vip, 0) + 1

    def _unpin_vip_session(self, vip: str):
        if not vip:
            return
        self.vip_session_refs[vip] = max(0, self.vip_session_refs.get(vip, 0) - 1)

    def _deterministic_client_vip(self, client_real_ip: str) -> Optional[str]:
        """Pick deterministic source VIP for a client at session creation."""
        return self.primary_vip.get(client_real_ip)

    def _session_key(self, src_ip: str, dst_vip: str, proto: int, src_port: int, dst_port: int) -> Tuple[str, str, int, int, int]:
        return (src_ip, dst_vip, proto, src_port, dst_port)

    def _expire_sessions(self, now: float):
        """Expire controller-side sessions.

        IMPORTANT:
        - TCP sessions: we use a state machine + expires_at to drive session_refs down
          when the TCP handshake/teardown is done or idle.
        - UDP sessions: we DO NOT expire based on a fixed controller timer.
          Instead, we rely on switch flow idle_timeout + FlowRemoved to remove sessions.
          UDP sessions are pinned (session_refs > 0) but removed when both flows expire,
          at which point VIPs are unpinned. This prevents GRACE VIPs from being reclaimed
          mid-UDP stream (e.g., long iperf -u sessions).
        """
        expired = []
        for key, sess in list(self.session_table.items()):
            proto = int(sess.get("proto", 0))

            # For UDP, never expire purely on controller timer; let flows/FlowRemoved
            # drive vip_flow_refs and thus VIP reclaim.
            if proto == socket.IPPROTO_UDP:
                continue

            expires_at = sess.get("expires_at", 0)
            if expires_at <= now:
                expired.append(key)

        for key in expired:
            sess = self.session_table.pop(key, None)
            if not sess:
                continue
            # Only TCP sessions reach here; unpin both VIPs so GRACE reclaim can happen
            self._unpin_vip_session(str(sess.get("client_src_vip", "")))
            self._unpin_vip_session(str(sess.get("server_reply_vip", "")))

    def _update_tcp_session_state(self, sess: Dict[str, object], tcp_pkt: Optional[tcp.tcp], now: float) -> bool:
        if not tcp_pkt:
            return False
        bits = int(tcp_pkt.bits)
        syn = bool(bits & tcp.TCP_SYN)
        ack = bool(bits & tcp.TCP_ACK)
        fin = bool(bits & tcp.TCP_FIN)
        rst = bool(bits & tcp.TCP_RST)

        entered_closing = False
        if rst or fin:
            entered_closing = sess.get("state") != self.SESSION_TCP_CLOSING
            sess["state"] = self.SESSION_TCP_CLOSING
            sess["expires_at"] = now + self.TCP_CLOSING_TIMEOUT
        elif syn and not ack and sess.get("state") != self.SESSION_TCP_ESTABLISHED:
            sess["state"] = self.SESSION_TCP_SYN_SEEN
            sess["expires_at"] = now + self.TCP_SYN_SEEN_TIMEOUT
        else:
            sess["state"] = self.SESSION_TCP_ESTABLISHED
            sess["expires_at"] = now + self.TCP_ESTABLISHED_TIMEOUT
        return entered_closing

    def _flow_idle_timeout_for_session(self, sess: Dict[str, object]) -> int:
        proto = int(sess["proto"])
        if proto == socket.IPPROTO_UDP:
            return self.UDP_ACTIVE_TIMEOUT
        state = str(sess.get("state", self.SESSION_TCP_SYN_SEEN))
        if state == self.SESSION_TCP_CLOSING:
            return self.TCP_CLOSING_TIMEOUT
        if state == self.SESSION_TCP_ESTABLISHED:
            return self.TCP_ESTABLISHED_TIMEOUT
        return self.TCP_SYN_SEEN_TIMEOUT

    def _extract_l4_info(self, pkt, ip4) -> Optional[Dict[str, object]]:
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        if tcp_pkt:
            return {
                "proto": socket.IPPROTO_TCP,
                "src_port": tcp_pkt.src_port,
                "dst_port": tcp_pkt.dst_port,
                "forward": {"ip_proto": socket.IPPROTO_TCP, "tcp_src": tcp_pkt.src_port, "tcp_dst": tcp_pkt.dst_port},
                "reverse": {"ip_proto": socket.IPPROTO_TCP, "tcp_src": tcp_pkt.dst_port, "tcp_dst": tcp_pkt.src_port},
                "tcp": tcp_pkt,
            }
        udp_pkt = pkt.get_protocol(udp.udp)
        if udp_pkt:
            return {
                "proto": socket.IPPROTO_UDP,
                "src_port": udp_pkt.src_port,
                "dst_port": udp_pkt.dst_port,
                "forward": {"ip_proto": socket.IPPROTO_UDP, "udp_src": udp_pkt.src_port, "udp_dst": udp_pkt.dst_port},
                "reverse": {"ip_proto": socket.IPPROTO_UDP, "udp_src": udp_pkt.dst_port, "udp_dst": udp_pkt.src_port},
                "tcp": None,
            }
        return None

    def _install_session_flows(self, msg, dp, parser, in_port, l4_info, sess, src_real_mac, dst_real_mac, rppt_start=None):
        """Install per-session flows for TCP/UDP.

        Cross-switch sessions keep VIPs in nw_dst/nw_src all the way through
        intermediate switches. Real-IP rewriting happens only at the two edge
        switches (last-hop DNAT at the server's switch, last-hop DNAT at the
        client's switch for the reverse direction).  Same-switch sessions do
        full SNAT+DNAT at the single shared switch as before.
        """
        if sess.get("flows_installed"):
            self.logger.debug(
                "SESSION_FLOW_SKIP: flows already installed for session "
                "(client_ip=%s server_vip=%s proto=%s src_port=%s dst_port=%s)",
                sess.get("client_real_ip"), sess.get("server_vip"),
                l4_info.get("proto"), l4_info.get("src_port"), l4_info.get("dst_port"),
            )
            client_ip = str(sess["client_real_ip"])
            client_vip = str(sess["client_src_vip"])
            server_real = str(sess["server_real_ip"])
            server_reply_vip = str(sess["server_reply_vip"])
            ofp = dp.ofproto
            ip4 = packet.Packet(msg.data).get_protocol(ipv4.ipv4)
            if ip4:
                # Route toward the server's edge by topology (no flooding on transit).
                dst_port = self._port_toward_host(dp.id, server_real)
                if dst_port is None:
                    dst_port = self.mac_to_port.get(dp.id, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
                # Use _is_final_delivery (checks both dpid AND port against host_location)
                # rather than _find_edge_switch_for_host, which can return a false positive
                # before switch_link_ports is fully built, causing an incorrect same-switch
                # path that leaks nw_dst=server_real onto inter-switch links.
                is_cross_switch = not self._is_final_delivery(dp.id, server_real, dst_port)
                src_port = self._port_toward_host(dp.id, client_ip)
                if src_port is None:
                    src_port = self.mac_to_port.get(dp.id, {}).get(src_real_mac, ofp.OFPP_FLOOD)
                if ip4.src == client_ip:
                    if is_cross_switch:
                        actions = [
                            parser.OFPActionSetField(ipv4_src=client_vip),
                            parser.OFPActionSetField(eth_src=self._ensure_vip_mac(client_vip)),
                            parser.OFPActionSetField(eth_dst=self._ensure_vip_mac(server_reply_vip)),
                            parser.OFPActionOutput(dst_port),
                        ]
                    else:
                        # Option 1 same-switch: server holds its VIP natively — no DNAT.
                        actions = [
                            parser.OFPActionSetField(ipv4_src=client_vip),
                            parser.OFPActionSetField(eth_src=self._ensure_vip_mac(client_vip)),
                            parser.OFPActionSetField(eth_dst=dst_real_mac),
                            parser.OFPActionOutput(dst_port),
                        ]
                    self._send_packet_out(msg, dp, in_port, actions)
                    # Install a flow so subsequent packets from the same session
                    # don't keep hitting PacketIn at this switch.  The session was
                    # first installed on the server's edge switch (same-switch mode
                    # when the client's MAC was not yet known there), so this switch
                    # never got its own forward/reverse flows.
                    idle_timeout = self._flow_idle_timeout_for_session(sess)
                    server_vip = str(sess["server_vip"])
                    match_fwd = parser.OFPMatch(
                        eth_type=0x0800,
                        ipv4_src=client_ip,
                        ipv4_dst=server_vip,
                        in_port=in_port,
                        **l4_info["forward"],
                    )
                    self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP,
                                   match=match_fwd, actions=actions,
                                   cookie=self._vip_cookie(client_vip),
                                   idle_timeout=idle_timeout)
                    if is_cross_switch:
                        # Install the client-edge reverse (DNAT) flow so reply
                        # packets don't keep hitting PacketIn here either.
                        match_rev = parser.OFPMatch(
                            eth_type=0x0800,
                            ipv4_src=server_reply_vip,
                            ipv4_dst=client_vip,
                            **l4_info["reverse"],
                        )
                        actions_rev = [
                            parser.OFPActionSetField(ipv4_dst=client_ip),
                            parser.OFPActionSetField(eth_dst=src_real_mac),
                            parser.OFPActionOutput(src_port),
                        ]
                        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP,
                                       match=match_rev, actions=actions_rev,
                                       cookie=self._vip_cookie(server_reply_vip),
                                       idle_timeout=idle_timeout)
                elif ip4.src == server_reply_vip:
                    # Already SNAT'd at server's edge — only DNAT nw_dst here
                    actions = [
                        parser.OFPActionSetField(ipv4_dst=client_ip),
                        parser.OFPActionSetField(eth_dst=src_real_mac),
                        parser.OFPActionOutput(src_port),
                    ]
                    self._send_packet_out(msg, dp, in_port, actions)
                elif ip4.src == server_real:
                    # Fallback: reply arrived without edge9 SNAT (same-switch or SNAT missing)
                    actions = [
                        parser.OFPActionSetField(ipv4_src=server_reply_vip),
                        parser.OFPActionSetField(ipv4_dst=client_ip),
                        parser.OFPActionSetField(eth_src=self._ensure_vip_mac(server_reply_vip)),
                        parser.OFPActionSetField(eth_dst=src_real_mac),
                        parser.OFPActionOutput(src_port),
                    ]
                    self._send_packet_out(msg, dp, in_port, actions)
            return True

        if rppt_start is None:
            rppt_start = time()
        ofp = dp.ofproto
        client_ip = str(sess["client_real_ip"])
        server_vip = str(sess["server_vip"])
        client_vip = str(sess["client_src_vip"])
        server_real = str(sess["server_real_ip"])
        server_reply_vip = str(sess["server_reply_vip"])

        src_vip_mac = self._ensure_vip_mac(client_vip)
        dst_vip_mac = self._ensure_vip_mac(server_reply_vip)
        if not src_vip_mac or not dst_vip_mac:
            self.logger.warning("SESSION: Missing VIP MAC(s) for %s -> %s", client_vip, server_reply_vip)
            return False

        idle_timeout = self._flow_idle_timeout_for_session(sess)
        # Route toward the server (forward) / client (reverse) edge by topology,
        # so transit switches send out one port instead of flooding.
        dst_port = self._port_toward_host(dp.id, server_real)
        if dst_port is None:
            dst_port = self.mac_to_port.get(dp.id, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
        src_port = self._port_toward_host(dp.id, client_ip)
        if src_port is None:
            src_port = self.mac_to_port.get(dp.id, {}).get(src_real_mac, ofp.OFPP_FLOOD)

        # Determine topology: same switch or cross-switch path.
        # _is_final_delivery checks that host_location[server_real] points to THIS
        # switch on THIS exact port — both conditions must hold.  This is stricter
        # than _find_edge_switch_for_host, which can return a false "same-switch"
        # result before switch_link_ports is fully built (e.g., host_location was
        # learned from a transit port and not yet purged).  A false same-switch
        # result causes a packet_out with nw_dst=server_real that leaks the real IP
        # onto inter-switch links, so we default to cross-switch when uncertain.
        is_cross_switch = not self._is_final_delivery(dp.id, server_real, dst_port)

        if is_cross_switch:
            # ── Forward ingress SNAT at client's edge ────────────────────────
            # nw_dst stays as server_vip — real-IP rewrite deferred to server's edge
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=client_vip),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_vip_mac),
                parser.OFPActionOutput(dst_port),
            ]
            # ── Reverse last-hop DNAT at client's edge ────────────────────────
            # Packet arrives from server's edge already SNAT'd: src=server_vip, dst=client_vip
            # Only rewrite nw_dst here.
            actions_rev = [
                parser.OFPActionSetField(ipv4_dst=client_ip),
                parser.OFPActionSetField(eth_dst=src_real_mac),
                parser.OFPActionOutput(src_port),
            ]
            match_rev = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=server_reply_vip,
                ipv4_dst=client_vip,
                **l4_info["reverse"],
            )
        else:
            # ── Same-switch: SNAT src→client_vip AND DNAT dst→server_real, so the
            # server receives a packet addressed to its real IP (iperf2 UDP needs
            # this to register the stream). Both hosts are on this one switch, so
            # nothing crosses the fabric and obfuscation is unaffected.
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=client_vip),
                parser.OFPActionSetField(ipv4_dst=server_real),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_real_mac),
                parser.OFPActionOutput(dst_port),
            ]
            # Reverse: server replies FROM its real IP; SNAT src→server_reply_vip
            # and DNAT dst→client real IP so the client sees its dialed VIP.
            actions_rev = [
                parser.OFPActionSetField(ipv4_src=server_reply_vip),
                parser.OFPActionSetField(ipv4_dst=client_ip),
                parser.OFPActionSetField(eth_src=dst_vip_mac),
                parser.OFPActionSetField(eth_dst=src_real_mac),
                parser.OFPActionOutput(src_port),
            ]
            match_rev = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=server_real,
                ipv4_dst=client_vip,
                **l4_info["reverse"],
            )

        match_fwd = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=client_ip,
            ipv4_dst=server_vip,
            in_port=in_port,
            **l4_info["forward"],
        )

        # Install last-hop flows on edge9 BEFORE sending the packet out so the
        # FlowMod reaches edge9 ahead of the first SYN.  If the packet arrived
        # first, edge9 would fall through to _handle_vip_to_vip, DNAT the packet
        # (correct), but also produce a stray "nw_dst=server_real" entry at
        # agg1/edge9 via _handle_vip_to_real.  Sending the FlowMod first closes
        # that race.
        if is_cross_switch:
            self._install_last_hop_flows(l4_info, sess, src_real_mac, dst_real_mac,
                                         idle_timeout, src_vip_mac, dst_vip_mac)
            # Proactively install the transit (intermediate-switch) flows along the
            # path NOW, so the first packet never PacketIns at the aggs/core. This
            # removes the reactive setup-window loss on deep campus paths. The flows
            # are VIP-only (same as the reactive ones), so obfuscation is unchanged.
            client_loc = self._find_edge_switch_for_host(client_ip)
            server_loc = self._find_edge_switch_for_host(server_real)
            if client_loc and server_loc:
                self._install_transit_flows(
                    client_loc[0], server_loc[0], client_vip, server_vip,
                    src_vip_mac, dst_vip_mac, l4_info, idle_timeout,
                )

        self.logger.info(
            "SESSION_FLOW_ADD_FWD: client_vip=%s server_vip=%s server_reply_vip=%s "
            "idle_timeout=%s match_src=%s match_dst=%s cross_switch=%s",
            client_vip, server_vip, server_reply_vip, idle_timeout, client_ip, server_vip, is_cross_switch,
        )
        # Flow-first, then release packet 0 through the pipeline (in order).
        # See _release_first_packet: this closes the ingress reorder window that
        # otherwise breaks iperf2 UDP stream init even though delivery is 100%.
        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP,
                       match=match_fwd, actions=actions_fwd,
                       cookie=self._vip_cookie(client_vip), idle_timeout=idle_timeout)
        self._release_first_packet(msg, dp, in_port)

        self.logger.info(
            "SESSION_FLOW_ADD_REV: client_vip=%s server_vip=%s server_reply_vip=%s "
            "idle_timeout=%s cross_switch=%s",
            client_vip, server_vip, server_reply_vip, idle_timeout, is_cross_switch,
        )
        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP,
                       match=match_rev, actions=actions_rev,
                       cookie=self._vip_cookie(server_reply_vip), idle_timeout=idle_timeout)

        rppt_key = (client_ip, server_vip, l4_info.get("proto"), l4_info.get("src_port"), l4_info.get("dst_port"))
        elapsed_ms = (time() - rppt_start) * 1000
        self.logger.info("RPPT_MEASURED: key=%s elapsed_ms=%.3f", rppt_key, elapsed_ms)
        sess["flows_installed"] = True
        return True

    def _is_final_delivery(self, dp_id: int, dst_real: str, out_port: int) -> bool:
        """Return True if out_port on dp_id is the direct link to dst_real host."""
        loc = self.host_location.get(dst_real)
        if loc is None or loc[0] != dp_id or loc[1] != out_port:
            return False
        # Guard: if out_port is a switch-to-switch link, host_location was
        # mis-learned from a transit hop — this is not a final-delivery port.
        uplink_ports = self.switch_link_ports.get(dp_id, set())
        return not (uplink_ports and out_port in uplink_ports)

    def _find_edge_switch_for_host(self, real_ip: str):
        """Return (dpid, port, mac) for the switch port directly facing real_ip.

        When switch_link_ports is built we scan mac_to_port for a non-uplink
        port, which is robust against host_location being set from a transit
        switch.  Falls back to host_location if the map is not yet available.
        """
        mac = self.host_ip_to_mac.get(real_ip)
        if self.switch_link_ports:
            # Try host_location first (fast path) — validate it's not an uplink.
            loc = self.host_location.get(real_ip)
            if loc:
                dpid, port, _ = loc
                if port not in self.switch_link_ports.get(dpid, set()):
                    return loc
            # host_location is absent or on an uplink; scan mac_to_port.
            if mac:
                for dpid, mac_ports in self.mac_to_port.items():
                    port = mac_ports.get(mac)
                    if port is None:
                        continue
                    if port not in self.switch_link_ports.get(dpid, set()):
                        return (dpid, port, mac)
            return None
        # switch_link_ports not yet built — use multi-MAC heuristic.
        # A port is an uplink if >1 distinct MAC arrives from it, since
        # multiple hosts' ARP/IP traverse uplinks while leaf ports face exactly
        # one host.  Pick the candidate with the fewest distinct MACs on that
        # port (most leaf-like).  Among ties, prefer the highest port number:
        # in this topology uplinks are port 1 (added first) and host ports are
        # 2…N (added after), so the highest-numbered port is the most leaf-like.
        # This prevents a transit switch's uplink (low port) from winning over
        # the server's true edge switch (high-numbered host port).
        if mac:
            best = None
            best_count = float("inf")
            for dpid, mac_ports in self.mac_to_port.items():
                port = mac_ports.get(mac)
                if port is None:
                    continue
                distinct_macs_on_port = sum(1 for p in mac_ports.values() if p == port)
                if distinct_macs_on_port < best_count or (
                        distinct_macs_on_port == best_count and (best is None or port > best[1])):
                    best_count = distinct_macs_on_port
                    best = (dpid, port, mac)
            if best is not None:
                self.logger.debug(
                    "EDGE_LOC: %s → dpid=%016x port=%d (count=%d via mac_to_port scan)",
                    real_ip, best[0], best[1], best_count)
                return best
        return self.host_location.get(real_ip)

    def _install_last_hop_flows(self, l4_info, sess, src_real_mac, dst_real_mac,
                                idle_timeout, src_vip_mac, dst_vip_mac):
        """Install DNAT/SNAT flows at the server's edge switch for cross-switch sessions.

        Forward: match(src=client_vip, dst=server_vip) → DNAT dst→server_real, output:server_port
        Reverse: match(src=server_real, dst=client_vip) → SNAT src→server_vip, output:agg_port
        agg_port is discovered via proactive ARP: edge9 learns client_real_mac → uplink port.
        """
        server_real = str(sess["server_real_ip"])
        client_vip = str(sess["client_src_vip"])
        server_reply_vip = str(sess["server_reply_vip"])

        loc = self._find_edge_switch_for_host(server_real)
        if not loc:
            self.logger.warning("LAST_HOP: No edge-switch location for server %s", server_real)
            return
        server_dpid, server_port, _ = loc
        server_dp = next((dp for dp in self.datapaths if dp.id == server_dpid), None)
        if not server_dp:
            self.logger.warning("LAST_HOP: No datapath for dpid=%016x", server_dpid)
            return

        sp = server_dp.ofproto_parser
        ofp = server_dp.ofproto

        # Reverse path leaves the server's edge toward the client's edge —
        # route by topology instead of relying on learned client MAC (avoids flood).
        agg_port = self._port_toward_host(server_dpid, str(sess["client_real_ip"]))
        if agg_port is None:
            agg_port = self.mac_to_port.get(server_dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)

        # Forward last-hop: DNAT the VIP dst → the server's REAL IP, then deliver.
        # The real IP only appears on the server's own access link (this final hop);
        # the whole fabric still carried VIP→VIP, so obfuscation is unchanged. The
        # server must receive a packet addressed to its real IP — Option-1's
        # "deliver dst=VIP" left iperf2's UDP server counting 0/0 (never registered).
        actions_fwd = [
            sp.OFPActionSetField(ipv4_dst=server_real),
            sp.OFPActionSetField(eth_dst=dst_real_mac),
            sp.OFPActionOutput(server_port),
        ]
        match_fwd = sp.OFPMatch(
            eth_type=0x0800,
            ipv4_src=client_vip,
            ipv4_dst=server_reply_vip,
            **l4_info["forward"],
        )
        self._add_flow(server_dp, priority=self.FLOW_PRIORITY_VIP,
                       match=match_fwd, actions=actions_fwd,
                       cookie=self._vip_cookie(server_reply_vip), idle_timeout=idle_timeout)

        # Reverse: the server now replies FROM its real IP (it received dst=real),
        # so match the real src and SNAT it back to the server VIP. The return path
        # leaves this edge as VIP→VIP, exactly as the fabric expects.
        actions_rev = [
            sp.OFPActionSetField(ipv4_src=server_reply_vip),
            sp.OFPActionSetField(eth_src=dst_vip_mac),
            sp.OFPActionSetField(eth_dst=src_vip_mac),
            sp.OFPActionOutput(agg_port),
        ]
        match_rev = sp.OFPMatch(
            eth_type=0x0800,
            ipv4_src=server_real,
            ipv4_dst=client_vip,
            **l4_info["reverse"],
        )
        self._add_flow(server_dp, priority=self.FLOW_PRIORITY_VIP,
                       match=match_rev, actions=actions_rev,
                       cookie=self._vip_cookie(client_vip), idle_timeout=idle_timeout)

        self.logger.info(
            "LAST_HOP: dp=%016x fwd(src=%s dst=%s port=%d) rev(src=%s dst=%s port=%d)",
            server_dpid, client_vip, server_reply_vip, server_port,
            server_reply_vip, client_vip, agg_port,
        )

    def _install_real_to_real_last_hop(self, fwd_l4, rev_l4, src_vip, dst_vip,
                                        src_vip_mac, dst_vip_mac, src_real_mac, dst_real_mac,
                                        dst_real, is_icmp_err):
        """Install DNAT/SNAT flows at the server's edge for real-to-real cross-switch sessions.

        Forward: match(src=client_vip, dst=server_vip) → DNAT dst→server_real
        Reverse: match(src=server_real, dst=client_vip) → SNAT src→server_vip, output:agg_port
        """
        loc = self._find_edge_switch_for_host(dst_real)
        if not loc:
            self.logger.warning("RTR_LAST_HOP: No edge-switch location for server %s", dst_real)
            return
        server_dpid, server_port, _ = loc
        server_dp = next((dp for dp in self.datapaths if dp.id == server_dpid), None)
        if not server_dp:
            self.logger.warning("RTR_LAST_HOP: No datapath for dpid=%016x", server_dpid)
            return

        sp = server_dp.ofproto_parser
        ofp = server_dp.ofproto

        # Reverse path leaves the server's edge toward the client — route by
        # topology (keyed off the client's MAC) instead of flooding.
        agg_port = self._port_toward_mac(server_dpid, src_real_mac)
        if agg_port is None:
            agg_port = self.mac_to_port.get(server_dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)

        actions_fwd = [
            sp.OFPActionSetField(ipv4_dst=dst_real),
            sp.OFPActionSetField(eth_dst=dst_real_mac),
            sp.OFPActionOutput(server_port),
        ]
        match_fwd = sp.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_vip,
            ipv4_dst=dst_vip,
            **fwd_l4,
        )
        cookie_fwd = 0 if is_icmp_err else self._vip_cookie(dst_vip)
        self._add_flow(server_dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                       actions=actions_fwd, cookie=cookie_fwd, idle_timeout=5)

        actions_rev = [
            sp.OFPActionSetField(ipv4_src=dst_vip),
            sp.OFPActionSetField(eth_src=dst_vip_mac),
            sp.OFPActionSetField(eth_dst=src_vip_mac),
            sp.OFPActionOutput(agg_port),
        ]
        match_rev = sp.OFPMatch(
            eth_type=0x0800,
            ipv4_src=dst_real,
            ipv4_dst=src_vip,
            **rev_l4,
        )
        cookie_rev = 0 if is_icmp_err else self._vip_cookie(src_vip)
        self._add_flow(server_dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                       actions=actions_rev, cookie=cookie_rev, idle_timeout=5)

        self.logger.info(
            "RTR_LAST_HOP: dp=%016x fwd(%s->%s->%s port=%d) rev(%s->%s<-%s port=%d)",
            server_dpid, src_vip, dst_vip, dst_real, server_port,
            dst_real, dst_vip, src_vip, agg_port,
        )

    def _find_reverse_session(self, server_real, client_src_vip, proto, server_port, client_port):
        """Find an existing session where this packet is the server's reply direction."""
        for sess in self.session_table.values():
            if (sess.get("server_real_ip") == server_real
                    and sess.get("client_src_vip") == client_src_vip
                    and sess.get("proto") == proto
                    and sess.get("server_port") == server_port
                    and sess.get("client_port") == client_port):
                return sess
        return None

    def _install_reverse_flow_on_switch(self, msg, dp, parser, in_port, l4_info, sess) -> bool:
        """Install the missing reverse NAT flow on the current switch (cross-switch case).

        Last-hop architecture:
        - At server's edge switch (edge9): SNAT only (src→server_vip, keep dst=client_vip).
          The client's edge switch already has a flow to DNAT dst→client_real.
        - At any other switch (agg1, or client edge as emergency fallback): full SNAT+DNAT
          so the packet reaches the client correctly in one translation step.
        """
        ofp = dp.ofproto
        server_real = str(sess["server_real_ip"])
        client_src_vip = str(sess["client_src_vip"])
        client_real = str(sess["client_real_ip"])
        server_reply_vip = str(sess["server_reply_vip"])

        server_reply_vip_mac = self._ensure_vip_mac(server_reply_vip)
        client_vip_mac = self._ensure_vip_mac(client_src_vip)
        idle_timeout = self._flow_idle_timeout_for_session(sess)

        # Is this the server's edge switch?
        server_loc = self._find_edge_switch_for_host(server_real)
        at_server_edge = server_loc is not None and server_loc[0] == dp.id

        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=server_real,
            ipv4_dst=client_src_vip,
            in_port=in_port,
            **l4_info["forward"],
        )

        if at_server_edge:
            # SNAT only: keep dst=client_vip so the client-edge DNAT flow handles the last mile.
            # Route toward the client's edge by topology instead of flooding.
            client_real_mac = self.host_ip_to_mac.get(client_real)
            agg_port = self._port_toward_host(dp.id, client_real)
            if agg_port is None:
                agg_port = (
                    self.mac_to_port.get(dp.id, {}).get(client_real_mac, ofp.OFPP_FLOOD)
                    if client_real_mac else ofp.OFPP_FLOOD
                )
            actions = [
                parser.OFPActionSetField(ipv4_src=server_reply_vip),
                parser.OFPActionSetField(eth_src=server_reply_vip_mac),
                parser.OFPActionSetField(eth_dst=client_vip_mac),
                parser.OFPActionOutput(agg_port),
            ]
            self.logger.info(
                "REVERSE_FLOW: SNAT-only at server_edge dp=%016x server=%s->%s dst=%s port=%d",
                dp.id, server_real, server_reply_vip, client_src_vip, agg_port,
            )
        else:
            # Full SNAT+DNAT at this intermediate or client-edge switch.
            client_real_mac = self.host_ip_to_mac.get(client_real)
            out_port = self._port_toward_host(dp.id, client_real)
            if out_port is None:
                out_port = (
                    self.mac_to_port.get(dp.id, {}).get(client_real_mac, ofp.OFPP_FLOOD)
                    if client_real_mac else ofp.OFPP_FLOOD
                )
            if client_real_mac and self._is_final_delivery(dp.id, client_real, out_port):
                rev_eth_dst = client_real_mac
            else:
                rev_eth_dst = client_vip_mac
            actions = [
                parser.OFPActionSetField(ipv4_src=server_reply_vip),
                parser.OFPActionSetField(ipv4_dst=client_real),
                parser.OFPActionSetField(eth_src=server_reply_vip_mac),
                parser.OFPActionSetField(eth_dst=rev_eth_dst),
                parser.OFPActionOutput(out_port),
            ]
            self.logger.info(
                "REVERSE_FLOW: full SNAT+DNAT at dp=%016x server=%s->%s client=%s->%s",
                dp.id, server_real, server_reply_vip, client_src_vip, client_real,
            )

        self._send_packet_out(msg, dp, in_port, actions)
        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match, actions=actions,
                       cookie=self._vip_cookie(server_reply_vip), idle_timeout=idle_timeout)

        now = time()
        self._touch_vip(client_src_vip, now)
        self._touch_vip(server_reply_vip, now)
        return True

    def _handle_l4_session(self, msg, dp, pkt, in_port, src_real, dst_vip, rppt_start=None) -> bool:
        parser = dp.ofproto_parser
        ip4 = pkt.get_protocol(ipv4.ipv4)
        if not ip4:
            return False
        l4_info = self._extract_l4_info(pkt, ip4)
        if not l4_info:
            return False

        # Handle TCP and UDP sessions (both use session table)
        proto = int(l4_info.get("proto", 0))
        real_dst = self.vip_owner.get(dst_vip)
        if not real_dst:
            return False

        src_real_mac = self.host_ip_to_mac.get(src_real)
        dst_real_mac = self.host_ip_to_mac.get(real_dst)
        if not src_real_mac or not dst_real_mac:
            return False

        now = time()
        key = self._session_key(src_real, dst_vip, proto, int(l4_info["src_port"]), int(l4_info["dst_port"]))
        sess = self.session_table.get(key)
        if not sess:
            # Check if this is a server reply arriving on a different switch than where the
            # original session flows were installed (cross-switch reverse path).
            # src_real = server, dst_vip = client_src_vip in the original session.
            reverse_sess = self._find_reverse_session(
                server_real=src_real,
                client_src_vip=dst_vip,
                proto=proto,
                server_port=int(l4_info["src_port"]),
                client_port=int(l4_info["dst_port"]),
            )
            if reverse_sess:
                return self._install_reverse_flow_on_switch(
                    msg, dp, parser, in_port, l4_info, reverse_sess
                )

            # A VIP in GRACE is still OWNED by its host and remains valid to serve.
            # A client's first packet to a server VIP that JUST rotated to GRACE is
            # legitimate in-flight traffic (the client resolved it microseconds before
            # the rotation) — black-holing it was dropping ~31/50 benchmark sessions
            # whenever a rotation coincided with traffic start. Only refuse if the VIP
            # is truly RECLAIMED (no longer owned). Reverse late-strays are still caught
            # by the _find_reverse_session check above and the existing-session check
            # below, so this does not revive a VIP from a stray reverse packet.
            if dst_vip not in self.vip_owner:
                self.logger.info(
                    "SESSION: Not creating session for unowned/reclaimed VIP %s "
                    "(late/stale packet from %s:%s -> %s:%s)",
                    dst_vip, src_real, l4_info.get("src_port"), dst_vip, l4_info.get("dst_port")
                )
                return False

            # Also check if there's an existing session involving the destination VIP.
            # Covers the reverse-stray case: the server sends late UDP packets back to the
            # client's old GRACE VIP (which is stored as client_src_vip, not server_vip).
            if self.vip_state.get(dst_vip) == self.VIP_STATE_GRACE:
                for existing_sess in self.session_table.values():
                    if (existing_sess.get("proto") == proto and
                        (existing_sess.get("server_vip") == dst_vip or
                         existing_sess.get("server_reply_vip") == dst_vip or
                         existing_sess.get("client_src_vip") == dst_vip)):
                        self.logger.info(
                            "SESSION: Dropping late packet to GRACE VIP %s (existing session client_src_vip=%s server_vip=%s) from %s",
                            dst_vip, existing_sess.get("client_src_vip"), existing_sess.get("server_vip"), src_real
                        )
                        return False

            client_vip = self._deterministic_client_vip(src_real)
            if not client_vip:
                return False
            is_udp = proto == socket.IPPROTO_UDP
            sess = {
                "client_real_ip": src_real,
                "server_vip": dst_vip,
                "proto": proto,
                "client_port": int(l4_info["src_port"]),
                "server_port": int(l4_info["dst_port"]),
                "client_src_vip": client_vip,
                "server_real_ip": real_dst,
                "server_reply_vip": dst_vip,
                "state": self.SESSION_UDP_ACTIVE if is_udp else self.SESSION_TCP_SYN_SEEN,
                "expires_at": now + (self.UDP_ACTIVE_TIMEOUT if is_udp else self.TCP_SYN_SEEN_TIMEOUT),
            }
            self.session_table[key] = sess
            # IMPORTANT:
            # - For both TCP and UDP, we pin VIP sessions so GRACE VIPs are not reclaimed
            #   while sessions are active. This protects VIPs during TCP handshake/teardown
            #   and ensures UDP VIPs stay active as long as the session exists.
            self._pin_vip_session(client_vip)
            self._pin_vip_session(dst_vip)

        self._touch_vip(str(sess.get("client_src_vip", "")), now)
        self._touch_vip(str(sess.get("server_reply_vip", "")), now)

        if proto == socket.IPPROTO_UDP:
            sess["state"] = self.SESSION_UDP_ACTIVE
            sess["expires_at"] = now + self.UDP_ACTIVE_TIMEOUT
        else:
            entered_closing = self._update_tcp_session_state(sess, l4_info.get("tcp"), now)
            if entered_closing and not sess.get("flows_deleted"):
                client_vip = str(sess.get("client_src_vip", ""))
                server_reply_vip = str(sess.get("server_reply_vip", ""))
                # CRITICAL: Touch both VIPs BEFORE deleting flows to start inactivity timer
                # This ensures both VIPs get reclaimed after 5s even if FlowRemoved events are delayed
                self._touch_vip(client_vip, now)
                self._touch_vip(server_reply_vip, now)
                self._delete_flows_by_cookie(client_vip)
                if server_reply_vip != client_vip:
                    self._delete_flows_by_cookie(server_reply_vip)
                sess["flows_deleted"] = True

        if sess.get("server_real_ip") != real_dst:
            sess["server_real_ip"] = real_dst

        return self._install_session_flows(msg, dp, parser, in_port, l4_info, sess, src_real_mac, dst_real_mac, rppt_start)

    # ---------------- topology discovery ----------------

    def _topology_counts(self) -> Tuple[int, int, int]:
        num_switches = len(self.datapaths)
        num_directed_links = 0
        try:
            sw = get_switch(self, None)
            ln = get_link(self, None)
            if sw is not None:
                num_switches = len(sw)
            if ln is not None:
                num_directed_links = len(ln)
        except Exception as e:
            self.logger.debug("TOPO_DISCOVERY: topology API not ready yet: %s", e)
        num_hosts = len(self.detected_hosts)
        return num_switches, num_directed_links, num_hosts

    def _maybe_complete_discovery(self, reason: str):
        if self.discovery_completed:
            return
        # Pick up the topology-expectations file if it was written after startup
        # (controller launched before the topology finished booting).
        self._load_topology_expectations()
        num_switches, num_directed_links, num_hosts = self._topology_counts()
        switches_ok = num_switches >= self.EXPECTED_SWITCHES
        links_ok = num_directed_links >= self.EXPECTED_DIRECTED_LINKS
        hosts_ok = True if self.EXPECTED_HOSTS <= 0 else num_hosts >= self.EXPECTED_HOSTS
        if switches_ok and links_ok and hosts_ok:
            self.discovery_end_time = time()
            self.discovery_completed = True
            self.discovery_completion_reason = reason
            elapsed = self.discovery_end_time - self.discovery_start_time
            self.logger.info(
                "TOPO_DISCOVERY_COMPLETE: elapsed=%.6f s | switches=%d | directed_links=%d | reason=%s",
                elapsed, num_switches, num_directed_links, reason,
            )
            self._build_switch_port_map()

    def _build_switch_port_map(self):
        """Build dpid→{uplink-ports} so host-location learning skips transit ports."""
        try:
            links = get_link(self, None)
            if not links:
                return
            port_map: Dict[int, Set[int]] = {}
            for link in links:
                port_map.setdefault(link.src.dpid, set()).add(link.src.port_no)
                port_map.setdefault(link.dst.dpid, set()).add(link.dst.port_no)
            self.switch_link_ports = port_map
            self.logger.info(
                "TOPO: uplink-port map built for %d switches: %s",
                len(port_map),
                {hex(k): sorted(v) for k, v in port_map.items()},
            )
            self._build_routing_table(links)
            # Purge host_location entries that were mis-learned at transit (uplink) ports
            # before this map was available.
            for real_ip, loc in list(self.host_location.items()):
                dpid, port, _ = loc
                if port in port_map.get(dpid, set()):
                    self.logger.warning(
                        "HOST_LOC_PURGE: %s mis-learned at dp=%016x port=%d (uplink) — removing",
                        real_ip, dpid, port,
                    )
                    del self.host_location[real_ip]
        except Exception as e:
            self.logger.warning("TOPO: failed to build uplink-port map: %s", e)

    def _build_routing_table(self, links):
        """Compute shortest-path next-hop ports between all switches (BFS).

        route_next_port[src][dst] = the local port on src to send out so the
        packet advances along a shortest path toward dst.  This lets transit
        switches forward VIP-tagged traffic toward the destination's edge
        switch by topology, instead of flooding (they never learn VIP MACs).
        """
        from collections import deque
        # adjacency: adj[dpid][neighbor_dpid] = local out-port toward neighbor.
        # Each Ryu Link carries BOTH endpoints' ports, so a single directed link
        # populates both directions. This makes the graph symmetric even if
        # get_link only reports one direction of a link (which happens
        # transiently, especially under load when LLDP packets are dropped).
        adj: Dict[int, Dict[int, int]] = {}
        port_peer: Dict[int, Dict[int, int]] = {}   # dpid -> {out_port: neighbor_dpid}
        for link in links:
            adj.setdefault(link.src.dpid, {})[link.dst.dpid] = link.src.port_no
            adj.setdefault(link.dst.dpid, {})[link.src.dpid] = link.dst.port_no
            port_peer.setdefault(link.src.dpid, {})[link.src.port_no] = link.dst.dpid
            port_peer.setdefault(link.dst.dpid, {})[link.dst.port_no] = link.src.dpid
        self._port_peer = port_peer
        route: Dict[int, Dict[int, int]] = {}
        for src in adj:
            route[src] = {}
            visited = {src}
            q = deque()
            for nbr, port in adj[src].items():
                route[src][nbr] = port
                visited.add(nbr)
                q.append((nbr, port))
            while q:
                cur, first_port = q.popleft()
                for nbr in adj.get(cur, {}):
                    if nbr not in visited:
                        visited.add(nbr)
                        route[src][nbr] = first_port  # same first hop reaches nbr
                        q.append((nbr, first_port))
        total_routes = sum(len(d) for d in route.values())
        prev = getattr(self, "_route_total", -1)
        self.route_next_port = route
        self._route_total = total_routes
        if total_routes != prev:
            self.logger.info(
                "TOPO: routing table built — %d switches, %d directed routes",
                len(route), total_routes,
            )

    def _port_toward_host(self, dpid: int, real_ip: str) -> Optional[int]:
        """Return the out-port on dpid to reach real_ip's host.

        - If dpid is the host's own edge switch, returns the host-facing port.
        - Otherwise returns the shortest-path next-hop port toward that edge.
        - Returns None if the location/route isn't known yet (caller floods).
        """
        loc = self._find_edge_switch_for_host(real_ip)
        if not loc:
            return None
        edge_dpid, host_port, _ = loc
        if dpid == edge_dpid:
            return host_port
        port = self.route_next_port.get(dpid, {}).get(edge_dpid)
        if port is None:
            self.logger.warning(
                "ROUTE_MISS: dp=%016x has no route to edge=%016x (host %s); "
                "known dests for this dp=%d — flooding as fallback",
                dpid, edge_dpid, real_ip, len(self.route_next_port.get(dpid, {})),
            )
        return port

    def _port_toward_mac(self, dpid: int, mac: Optional[str]) -> Optional[int]:
        """Like _port_toward_host but keyed by a host's MAC (when only the MAC
        is known, e.g. reverse-path agg ports). Returns None if unknown."""
        if not mac:
            return None
        for ip, m in self.host_ip_to_mac.items():
            if m == mac:
                return self._port_toward_host(dpid, ip)
        return None

    def _transit_dpids(self, src_edge: int, dst_edge: int) -> List[int]:
        """Ordered list of INTERMEDIATE switch dpids strictly between two edges,
        following the shortest path. Excludes both edge switches.

        Walks route_next_port (out-port per hop) + _port_peer (port -> neighbor).
        Returns [] for same-switch / adjacent edges or if the path is unknown.
        """
        if src_edge == dst_edge:
            return []
        hops: List[int] = []
        cur = src_edge
        guard = 0
        while cur != dst_edge and guard < 64:
            guard += 1
            port = self.route_next_port.get(cur, {}).get(dst_edge)
            if port is None:
                return []  # route unknown — caller relies on reactive fallback
            nxt = self._port_peer.get(cur, {}).get(port)
            if nxt is None:
                return []
            if nxt != dst_edge:
                hops.append(nxt)
            cur = nxt
        return hops

    def _install_transit_flows(self, client_edge, server_edge, client_vip, server_vip,
                               client_vip_mac, server_vip_mac, l4_info, idle_timeout):
        """Proactively install the forward+reverse transit flows on every switch
        BETWEEN the client and server edges, at session setup.

        These are byte-for-byte the flows _handle_vip_to_vip would install
        reactively (VIP-only match, set eth_dst + output toward the next hop) —
        so the real-IP obfuscation is unchanged; only the timing differs. Doing
        it up front means the first packet never PacketIns at the intermediates,
        eliminating the reactive setup-window loss (the UDP/ICMP loss on deep
        campus paths).
        """
        transit = self._transit_dpids(client_edge, server_edge)
        for dpid in transit:
            dp = next((d for d in self.datapaths if d.id == dpid), None)
            if dp is None:
                continue
            fwd_port = self.route_next_port.get(dpid, {}).get(server_edge)
            rev_port = self.route_next_port.get(dpid, {}).get(client_edge)
            if fwd_port is None or rev_port is None:
                continue  # incomplete route for this hop — leave it to reactive
            parser = dp.ofproto_parser
            # Forward: client_vip -> server_vip, steer toward server edge.
            match_fwd = parser.OFPMatch(eth_type=0x0800, ipv4_src=client_vip,
                                        ipv4_dst=server_vip, **l4_info["forward"])
            actions_fwd = [parser.OFPActionSetField(eth_dst=server_vip_mac),
                           parser.OFPActionOutput(fwd_port)]
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                           actions=actions_fwd, cookie=self._vip_cookie(server_vip),
                           idle_timeout=idle_timeout)
            # Reverse: server_vip -> client_vip, steer toward client edge.
            match_rev = parser.OFPMatch(eth_type=0x0800, ipv4_src=server_vip,
                                        ipv4_dst=client_vip, **l4_info["reverse"])
            actions_rev = [parser.OFPActionSetField(eth_dst=client_vip_mac),
                           parser.OFPActionOutput(rev_port)]
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                           actions=actions_rev, cookie=self._vip_cookie(client_vip),
                           idle_timeout=idle_timeout)
        if transit:
            self.logger.info(
                "TRANSIT_PREINSTALL: %d intermediate switch(es) for %s<->%s: %s",
                len(transit), client_vip, server_vip,
                [hex(d) for d in transit],
            )

    def _log_discovery_progress(self, reason: str):
        if self.discovery_completed:
            return
        num_switches, num_directed_links, num_hosts = self._topology_counts()
        elapsed = time() - self.discovery_start_time
        self.logger.info(
            "TOPO_DISCOVERY_PROGRESS: elapsed=%.6f s | switches=%d/%d | directed_links=%d/%d | reason=%s",
            elapsed, num_switches, self.EXPECTED_SWITCHES, num_directed_links, self.EXPECTED_DIRECTED_LINKS, reason,
        )

    @set_ev_cls(topo_event.EventSwitchEnter)
    def _on_topology_switch_enter(self, ev):
        self._log_discovery_progress("EventSwitchEnter")
        self._maybe_complete_discovery("EventSwitchEnter")

    @set_ev_cls(topo_event.EventLinkAdd)
    def _on_topology_link_add(self, ev):
        # Populate switch_link_ports incrementally — each EventLinkAdd carries
        # the exact src/dst dpid+port, so we never need get_link() to work.
        link = ev.link
        self.switch_link_ports.setdefault(link.src.dpid, set()).add(link.src.port_no)
        self.switch_link_ports.setdefault(link.dst.dpid, set()).add(link.dst.port_no)
        self.logger.debug(
            "LINK_ADD: %016x:%d <-> %016x:%d (uplink map now covers %d switches)",
            link.src.dpid, link.src.port_no, link.dst.dpid, link.dst.port_no,
            len(self.switch_link_ports),
        )
        self._log_discovery_progress("EventLinkAdd")
        self._maybe_complete_discovery("EventLinkAdd")

    # ---------------- switch bringup ----------------

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        self.datapaths.add(dp)
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp, priority=0, match=match, actions=actions, table_id=0, idle_timeout=0)
        self.logger.info("[SW] Switch %016x connected; installed table-miss", dp.id)
        self._log_discovery_progress("EventOFPSwitchFeatures")
        self._maybe_complete_discovery("EventOFPSwitchFeatures")

    # ---------------- rotation ----------------

    def _rotation_loop(self):
        while True:
            self.logger.debug("ROTATE: sleeping for %ss before next primary VIP rotation", self.ROTATE_INTERVAL)
            hub.sleep(self.ROTATE_INTERVAL)
            now = time()
            hosts = sorted(self.detected_hosts)
            self.logger.info("ROTATE: starting rotation for %d hosts (batch=%d delay=%.2fs)",
                             len(hosts), self.ROTATION_BATCH_SIZE, self.ROTATION_BATCH_DELAY)
            for idx, host_ip in enumerate(hosts):
                if idx > 0 and idx % self.ROTATION_BATCH_SIZE == 0:
                    hub.sleep(self.ROTATION_BATCH_DELAY)
                old_vip = self.primary_vip.get(host_ip)
                new_vip = self._take_resource_vip()
                if not new_vip:
                    self.logger.warning("ROTATE: no VIP available for %s", host_ip)
                    continue
                self._bind_primary_vip(host_ip, new_vip, now)
                if old_vip and old_vip != new_vip:
                    # Safety check: Ensure old_vip was actually PRIMARY before moving to GRACE
                    if self.vip_state.get(old_vip) != self.VIP_STATE_PRIMARY:
                        self.logger.warning("ROTATE: Old VIP %s is not PRIMARY (state=%s), skipping GRACE transition",
                                           old_vip, self.vip_state.get(old_vip))
                        continue
                    
                    self.vip_state[old_vip] = self.VIP_STATE_GRACE
                    # Check if VIP is idle/active using both dataplane flows and controller sessions
                    flow_refs = self.vip_flow_refs.get(old_vip, 0)
                    session_refs = self.vip_session_refs.get(old_vip, 0)
                    if flow_refs <= 0 and session_refs <= 0:
                        self.logger.info(
                            "ROTATE: host=%s new=%s old=%s -> GRACE (idle), reclaim eligible after %ss inactivity",
                            host_ip,
                            new_vip,
                            old_vip,
                            self.VIP_INACTIVITY_RECLAIM,
                        )
                    else:
                        # VIP has active sessions/flows - keep in GRACE until both end
                        self.logger.info(
                            "ROTATE: host=%s new=%s old=%s -> GRACE (active, %d flows, %d sessions), will reclaim when flows/sessions end",
                            host_ip,
                            new_vip,
                            old_vip,
                            flow_refs,
                            session_refs,
                        )

    # ---------------- host discovery ----------------

    def _is_synthetic_vip_mac(self, mac: Optional[str]) -> bool:
        """True if mac is one of our generated VIP MACs (02:.. locally-administered).

        Real host MACs never start with 02: (see _generate_vip_mac). We must NOT
        learn a synthetic VIP MAC as a host's REAL MAC: that happens when a
        controller-SNAT'd packet (src=VIP, eth_src=VIP_MAC) hits PacketIn, and it
        corrupts host_ip_to_mac so last-hop delivery uses the wrong dst MAC and
        the destination host drops the frame at L2 (UDP/TCP silently fail).
        """
        return bool(mac) and mac.lower().startswith("02:")

    def _learn_host(self, pkt, dpid: int, in_port: Optional[int] = None):
        """Learn host from ARP or IP packet across the full managed real-host space."""
        eth_pkt = pkt.get_protocol(ethernet.ethernet)
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        real_ip, mac = None, None
        if arp_pkt:
            real_ip, mac = arp_pkt.src_ip, arp_pkt.src_mac
        elif ip_pkt and eth_pkt:
            real_ip, mac = ip_pkt.src, eth_pkt.src
        else:
            return

        # Option 1: hosts send with VIP as their data-plane IP.  Learn the real
        # host's MAC address and edge-switch location from VIP-sourced packets.
        # BUT skip packets whose eth_src is a synthetic VIP MAC — those are
        # controller-SNAT'd, not host-originated, and would corrupt host_ip_to_mac.
        if (real_ip and real_ip in self.vip_owner and mac and in_port is not None
                and not self._is_synthetic_vip_mac(mac)):
            actual_real = self.vip_owner[real_ip]
            self.host_ip_to_mac[actual_real] = mac
            uplink_ports = self.switch_link_ports.get(dpid, set())
            if uplink_ports:
                if in_port not in uplink_ports:
                    self._remember_host_location(actual_real, dpid, in_port, mac)
            else:
                cur_map = self.mac_to_port.get(dpid, {})
                distinct_macs_on_port = sum(1 for p in cur_map.values() if p == in_port)
                if distinct_macs_on_port <= 1:
                    self._remember_host_location(actual_real, dpid, in_port, mac)

        if not real_ip or not self._ip_in_real_host_space(real_ip):
            return

        # Never record a synthetic VIP MAC as a real host's MAC (see above).
        if mac and not self._is_synthetic_vip_mac(mac):
            self.host_ip_to_mac[real_ip] = mac
            if in_port is not None:
                # Only record the host's edge location when arriving from a host-facing port.
                # ARP replies traverse multiple switches; recording at transit (uplink) ports
                # would make _is_final_delivery incorrectly return True on intermediate switches.
                uplink_ports = self.switch_link_ports.get(dpid, set())
                if uplink_ports:
                    # switch_link_ports is built — trust it directly.
                    is_uplink = in_port in uplink_ports
                else:
                    # Fallback: port is an uplink if >1 distinct MAC arrives from it.
                    # Uplinks carry traffic from many hosts; leaf ports face exactly one.
                    cur_map = self.mac_to_port.get(dpid, {})
                    distinct_macs_on_port = sum(1 for m, p in cur_map.items() if p == in_port)
                    is_uplink = distinct_macs_on_port > 1
                if not is_uplink:
                    self._remember_host_location(real_ip, dpid, in_port, mac)

        if real_ip not in self.detected_hosts:
            self.detected_hosts.add(real_ip)
            self.host_vip_pools.setdefault(real_ip, set())
            now = time()
            new_vip = self._take_resource_vip()
            if new_vip:
                self._bind_primary_vip(real_ip, new_vip, now)
                self.logger.info(
                    "[+] New host %s (%s) on switch=%016x port=%s - assigned VIP: %s",
                    real_ip, mac, dpid, in_port, new_vip
                )

    def _send_arp_reply(self, dp, dst_mac, src_mac, src_ip, target_ip, out_port):
        """Send ARP reply."""
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        p = packet.Packet()
        p.add_protocol(ethernet.ethernet(
            ethertype=0x0806,
            dst=dst_mac,
            src=src_mac
        ))
        p.add_protocol(arp.arp(
            opcode=arp.ARP_REPLY,
            src_mac=src_mac,
            src_ip=src_ip,
            dst_mac=dst_mac,
            dst_ip=target_ip
        ))
        p.serialize()
        dp.send_msg(parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofp.OFP_NO_BUFFER,
            in_port=ofp.OFPP_CONTROLLER,
            actions=[parser.OFPActionOutput(out_port)],
            data=p.data
        ))

    def _proactive_discovery(self, now: float):
        """Proactively send ARP requests in batches across the managed real-host space."""
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

        # OBFUSCATION: discovery ARPs must reach HOSTS without crossing the fabric.
        # Flooding them (OFPP_FLOOD) sends real host IPs (and the resulting ARP
        # replies, carrying real IP+MAC) across inter-switch links — a reconnaissance
        # leak on the backbone. Instead, compute each switch's ACCESS (host-facing)
        # ports = all ports minus the known inter-switch uplinks, and emit the probe
        # ONLY there. A switch whose uplinks/ports aren't known yet is skipped; we
        # never fall back to FLOOD.
        access_ports = {}
        try:
            for _sw in get_switch(self, None):
                _d = _sw.dp.id
                _up = self.switch_link_ports.get(_d, set())
                if not _up:
                    continue  # uplinks unknown -> can't safely exclude them
                _hp = [p.port_no for p in _sw.ports
                       if p.port_no not in _up and p.port_no < 0xff00]
                if _hp:
                    access_ports[_d] = _hp
        except Exception as e:
            self.logger.debug("Discovery: access-port computation failed: %s", e)
            access_ports = {}
        if not access_ports:
            return  # topology not mapped yet — don't probe (and never FLOOD)

        for target_ip in batch:
            # Skip only hosts we've already LOCATED. Option-1 bootstrap marks every
            # host "detected" (from ns_map) but WITHOUT a port, so host_location is
            # empty until the host sends traffic. A never-active host then has no
            # location, and same-switch delivery to it mis-detects as cross-switch
            # (wrong eth_dst → frame dropped). ARPing unlocated hosts makes them
            # reply, which teaches host_location and fixes cold-start connectivity.
            if target_ip in self.detected_hosts and target_ip in self.host_location:
                continue

            if target_ip in self._last_discovery and (now - self._last_discovery[target_ip] < 60):
                continue

            self._last_discovery[target_ip] = now

            for dp in list(self.datapaths):
                hp = access_ports.get(dp.id)
                if not hp:
                    continue  # no known host-facing ports on this switch -> skip
                try:
                    parser = dp.ofproto_parser
                    ofp = dp.ofproto
                    p = packet.Packet()
                    p.add_protocol(ethernet.ethernet(
                        ethertype=0x0806,
                        dst='ff:ff:ff:ff:ff:ff',
                        src=self.CONTROLLER_DISCOVERY_MAC
                    ))
                    p.add_protocol(arp.arp(
                        opcode=arp.ARP_REQUEST,
                        src_mac=self.CONTROLLER_DISCOVERY_MAC,
                        src_ip=self.CONTROLLER_DISCOVERY_IP,
                        dst_mac='00:00:00:00:00:00',
                        dst_ip=target_ip
                    ))
                    p.serialize()
                    # Output ONLY to host-facing ports (never an uplink / OFPP_FLOOD),
                    # so the probe reaches local hosts but never crosses the fabric.
                    actions = [parser.OFPActionOutput(pp) for pp in hp]
                    dp.send_msg(parser.OFPPacketOut(
                        datapath=dp,
                        buffer_id=ofp.OFP_NO_BUFFER,
                        in_port=ofp.OFPP_CONTROLLER,
                        actions=actions,
                        data=p.data
                    ))
                except Exception as e:
                    self.logger.debug("Discovery ARP to %s failed on dp=%016x: %s", target_ip, dp.id, e)

        if end_idx >= total:
            self._discovery_cursor = 1
        else:
            self._discovery_cursor = end_idx + 1

    # ---------------- logging ----------------

    def _log_vip_pools(self, now: float):
        """Log VIP pools per host."""
        self.logger.info("=== VIP POOLS ===")
        
        def ipkey(ip):
            try:
                return tuple(int(x) for x in ip.split('.'))
            except Exception:
                return (ip,)

        total = 0
        active_total = 0
        for real_ip in sorted(self.detected_hosts, key=ipkey):
            pool = self.host_vip_pools.get(real_ip, set())
            if not pool:
                self.logger.info("Host %s: No VIPs assigned", real_ip)
                continue

            self.logger.info("Host %s (%d VIPs):", real_ip, len(pool))
            self.logger.info(" %-13s %-9s %-15s", "VIP", "Uptime", "State")
            self.logger.info(" %-13s %-9s %-15s", "-------------", "---------", "---------------")

            host_active = 0
            for vip in sorted(pool, key=ipkey):
                created = self.vip_created_at.get(vip, now)
                uptime = f"{max(0.0, (now - created)):.1f}s"
                state = self.vip_state.get(vip, "UNKNOWN")
                
                # Mark as ACTIVE if VIP has active flows (flow_refs > 0) OR pinned sessions (session_refs > 0)
                flow_refs = self.vip_flow_refs.get(vip, 0)
                session_refs = self.vip_session_refs.get(vip, 0)
                is_active = flow_refs > 0 or session_refs > 0
                
                # For UDP sessions: If server VIP has no flows but client VIP (forward flow) is still active,
                # mark server VIP as active. This handles cases where reverse flow expires but forward flow continues.
                if not is_active:
                    for sess in self.session_table.values():
                        if (sess.get("server_reply_vip") == vip and 
                            sess.get("proto") == socket.IPPROTO_UDP and
                            sess.get("expires_at", 0) > now):
                            client_vip = sess.get("client_src_vip")
                            if client_vip and self.vip_flow_refs.get(client_vip, 0) > 0:
                                is_active = True
                                break
                
                if is_active:
                    host_active += 1
                    active_total += 1
                    state_display = f"{state}/ACTIVE"
                else:
                    state_display = f"{state}/IDLE"
                self.logger.info(" %-13s %-9s %-15s", vip, uptime, state_display)
            total += len(pool)
            self.logger.info(" → %d active, %d idle", host_active, len(pool) - host_active)

        self.logger.info("=== SUMMARY: %d total VIPs (%d active, %d idle) ===",
                         total, active_total, total - active_total)

    # ---------------- VIP reclamation ----------------

    def _reclaim_vip(self, vip: str):
        """Reclaim a VIP and move it into quarantine before resource reuse."""
        # Safety check: Never reclaim PRIMARY VIPs - they should only be rotated
        if self.vip_state.get(vip) == self.VIP_STATE_PRIMARY:
            self.logger.warning("RECLAIM: Attempted to reclaim PRIMARY VIP %s - this should not happen! Skipping.", vip)
            return
        
        owner = self.vip_owner.pop(vip, None)
        if not owner:
            return

        # Option 1: GRACE VIP has expired — remove it from the host interface now.
        self._remove_vip_from_host(owner, vip)

        if owner in self.host_vip_pools:
            self.host_vip_pools[owner].discard(vip)

        self.vip_state.pop(vip, None)
        self.vip_mac_map.pop(vip, None)
        self.vip_created_at.pop(vip, None)
        self.vip_flow_refs.pop(vip, None)
        self.vip_flow_keys.pop(vip, None)
        self.vip_session_refs.pop(vip, None)
        self.vip_delete_requested_at.pop(vip, None)

        if self.primary_vip.get(owner) == vip:
            self.primary_vip.pop(owner, None)

        now = time()
        last_seen = self.vip_last_seen.pop(vip, now)
        lived_ms = (now - last_seen) * 1000
        self.quarantine_until[vip] = now + self.VIP_QUARANTINE_SECONDS

        self.logger.info("VIP_RECLAIMED: vip=%s lived_ms=%.3f", vip, lived_ms)
        self.logger.info("RECLAIM: VIP %s from host %s -> quarantine %ss", vip, owner, self.VIP_QUARANTINE_SECONDS)
        # Update DNS mapping file
        self._update_dns_mapping()

    def _update_dns_mapping(self):
        """
        Update DNS mapping file for DNS server.
        
        Creates mapping: {"real_ip": "primary_vip", ...}
        Example: {"10.0.0.1": "10.0.0.51", "10.0.0.2": "10.0.0.52"}
        
        DNS server reads this file to resolve hostnames (h1, h2, etc.) to
        current PRIMARY VIPs. This file is updated:
        - When VIP is bound (initial assignment)
        - When VIP rotates (every 60s)
        - When VIP is reclaimed
        
        DNS server reloads this file on each query to always return the latest
        PRIMARY VIPs, which rotate every ROTATE_INTERVAL (60s).
        """
        import json
        import os
        
        # Map real IPs to their current PRIMARY VIPs
        mapping = {}
        for host_ip, vip in self.primary_vip.items():
            if vip:  # Only include hosts with active PRIMARY VIPs
                mapping[host_ip] = vip
        
        # Write to shared file (adjust path for Windows if needed)
        mapping_file = "/tmp/mtd_vip_mapping.json"
        if os.name == 'nt':  # Windows
            mapping_file = os.path.join(os.environ.get('TEMP', 'C:\\temp'), 'mtd_vip_mapping.json')
        
        try:
            with open(mapping_file, 'w') as f:
                json.dump(mapping, f)
            self.logger.debug("DNS: Updated mapping file with %d entries", len(mapping))
        except PermissionError:
            # Stale file owned by a previous root session — remove and recreate.
            try:
                os.remove(mapping_file)
                with open(mapping_file, 'w') as f:
                    json.dump(mapping, f)
                self.logger.debug("DNS: Recreated mapping file with %d entries", len(mapping))
            except Exception as e2:
                self.logger.warning("DNS: Failed to update mapping file: %s", e2)
        except Exception as e:
            self.logger.warning("DNS: Failed to update mapping file: %s", e)

    # ---------------- packet handling ----------------

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        # RPPT: start timing at PacketIn (same as baseline) so both measure PacketIn -> FlowMod
        rppt_start = time()
        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        in_port = msg.match['in_port']
        dpid = dp.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if not eth:
            return

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][eth.src] = in_port

        # Handle ARP
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self._learn_host(pkt, dpid, in_port)
            self._handle_arp(msg, dp, pkt, arp_pkt, eth, in_port, dpid)
            return

        # Handle IP packets
        ip4 = pkt.get_protocol(ipv4.ipv4)
        if not ip4:
            return

        self._learn_host(pkt, dpid, in_port)
        src_ip, dst_ip = ip4.src, ip4.dst

        # DNS-based approach: 
        # - Hosts resolve destinations to VIPs via DNS
        # - Hosts send: src=real_ip, dst=VIP
        # - Controller does SNAT (real→VIP) and DNAT (VIP→real)
        # - Much simpler than original: no complex session tracking
        
        src_is_real = src_ip in self.detected_hosts
        dst_is_real = dst_ip in self.detected_hosts
        dst_is_vip = dst_ip in self.vip_owner
        src_is_vip = src_ip in self.vip_owner

        # Real host → Real host: Translate both to VIPs (when hosts use real IPs directly)
        if src_is_real and dst_is_real:
            self._handle_real_to_real(msg, dp, pkt, ip4, eth, in_port, dpid, src_ip, dst_ip)
            return

        # Real host → VIP: SNAT + DNAT (when DNS resolves to VIP)
        if src_is_real and dst_is_vip:
            self._handle_real_to_vip(msg, dp, pkt, ip4, eth, in_port, dpid, src_ip, dst_ip, rppt_start)
            return

        # VIP → Real host: Reverse SNAT (reply path)
        # Must check not dst_is_vip first — VIP-to-VIP transit packets also have src_is_vip=True
        # and would incorrectly enter this branch before reaching _handle_vip_to_vip.
        if src_is_vip and src_ip in self.vip_owner and not dst_is_vip:
            self._handle_vip_to_real(msg, dp, pkt, ip4, eth, in_port, dpid, src_ip, dst_ip)
            return

        # VIP → VIP: Both already translated
        if src_is_vip and dst_is_vip:
            self._handle_vip_to_vip(msg, dp, pkt, ip4, eth, in_port, dpid, src_ip, dst_ip, rppt_start)
            return

        # Unknown: forward as-is
        self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)

    def _vip_is_active_for_arp(self, vip: str, now: Optional[float] = None) -> bool:
        """Return True if a VIP should still answer ARP.

        PRIMARY VIPs always answer ARP. GRACE VIPs answer ARP only while they
        are still active for an existing session/flow. This preserves session
        continuity without allowing inactive old VIPs to remain discoverable.
        """
        if not vip or vip not in self.vip_owner:
            return False

        state = self.vip_state.get(vip)
        if state == self.VIP_STATE_PRIMARY:
            return True
        if state != self.VIP_STATE_GRACE:
            return False

        if self.vip_flow_refs.get(vip, 0) > 0:
            return True
        if self.vip_session_refs.get(vip, 0) > 0:
            return True

        now = now if now is not None else time()

        # UDP special case: keep server GRACE VIP ARP-reachable while the
        # client-side forward flow is still alive, even if the reverse flow has
        # already expired.
        for sess in self.session_table.values():
            if (
                sess.get("server_reply_vip") == vip
                and sess.get("proto") == socket.IPPROTO_UDP
                and sess.get("expires_at", 0) > now
            ):
                client_vip = sess.get("client_src_vip")
                if client_vip and self.vip_flow_refs.get(client_vip, 0) > 0:
                    return True

        return False

    def _handle_arp(self, msg, dp, pkt, arp_pkt, eth, in_port, dpid):
        """Handle ARP with session-aware reachability for GRACE VIPs."""
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        if arp_pkt.opcode == arp.ARP_REQUEST:
            target_ip = arp_pkt.dst_ip

            if target_ip in self.vip_owner:
                vip_state = self.vip_state.get(target_ip)
                vip_mac = self.vip_mac_map.get(target_ip)

                if not vip_mac:
                    self.logger.warning("ARP: no VIP MAC for %s (state=%s)", target_ip, vip_state)
                    return

                # DISABLED: Guard that suppressed ARP replies for idle GRACE VIPs.
                # All known VIPs now always answer ARP so late reverse packets
                # (e.g. final UDP ack) can still reach the client.
                # if self._vip_is_active_for_arp(target_ip):
                self._send_arp_reply(
                    dp, eth.src, vip_mac, target_ip, arp_pkt.src_ip, in_port
                )
                self.logger.debug(
                    "ARP: replied VIP %s -> %s (state=%s flow_refs=%d session_refs=%d)",
                    target_ip,
                    vip_mac,
                    vip_state,
                    self.vip_flow_refs.get(target_ip, 0),
                    self.vip_session_refs.get(target_ip, 0),
                )
                # else:
                #     self.logger.info(
                #         "ARP: suppressing reply for VIP %s (state=%s flow_refs=%d session_refs=%d)",
                #         target_ip,
                #         vip_state,
                #         self.vip_flow_refs.get(target_ip, 0),
                #         self.vip_session_refs.get(target_ip, 0),
                #     )
                return

            # Forward ARP requests for real hosts (let them resolve normally)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        # Forward ARP replies
        out_port = self.mac_to_port.get(dpid, {}).get(eth.dst, ofp.OFPP_FLOOD)
        self._forward_packet(msg, dp, in_port, dpid, eth.dst, out_port)

    def _handle_real_to_real(self, msg, dp, pkt, ip4, eth, in_port, dpid, src_real, dst_real):
        """Handle direct real-IP traffic using the last-hop VIP architecture.

        TCP/UDP and ICMP/other are all handled uniformly by the last-hop code
        below: SNAT src→client_vip and DNAT dst→server_vip at the client's edge,
        keep VIPs in nw_src/nw_dst across the fabric, and rewrite back to the real
        IP only at the server's edge switch.  L4 port matching is included via
        _extract_l4_match_fields, so TCP/UDP sessions match correctly.

        Do NOT redirect TCP/UDP to _handle_real_to_vip: that path builds flows
        matching ipv4_dst=server_vip, but a real→real packet carries
        ipv4_dst=server_real.  The flows would never match, so every packet would
        keep its real dst IP and leak it onto the fabric (and the connection would
        stall on retransmits).
        """
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        src_vip = self.primary_vip.get(src_real)
        dst_vip = self.primary_vip.get(dst_real)

        if not src_vip or not dst_vip:
            self.logger.warning("REAL-TO-REAL: Missing VIP for src=%s or dst=%s", src_real, dst_real)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        src_vip_mac = self._ensure_vip_mac(src_vip)
        dst_vip_mac = self._ensure_vip_mac(dst_vip)
        src_real_mac = self.host_ip_to_mac.get(src_real) or eth.src
        dst_real_mac = self.host_ip_to_mac.get(dst_real)

        if not src_vip_mac:
            self.logger.warning("REAL-TO-REAL: Missing VIP MAC for src_vip=%s", src_vip)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        if not dst_real_mac:
            self.logger.debug("REAL-TO-REAL: Destination MAC unknown for %s, flooding", dst_real)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        if src_real_mac:
            self.host_ip_to_mac[src_real] = src_real_mac

        forward_l4_match, reverse_l4_match = self._extract_l4_match_fields(pkt, ip4)
        is_icmp_err = self._is_icmp_error(pkt)

        # Route toward the destination's edge by topology (no transit flooding).
        dst_port = self._port_toward_host(dpid, dst_real)
        if dst_port is None:
            dst_port = self.mac_to_port.get(dpid, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
        is_cross_switch = not self._is_final_delivery(dpid, dst_real, dst_port)

        if is_cross_switch:
            # ── Forward at client's edge: SNAT src→client_vip, set dst→server_vip ─────
            # nw_dst becomes server_vip — real IP is never exposed on inter-switch links.
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=src_vip),
                parser.OFPActionSetField(ipv4_dst=dst_vip),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_vip_mac),
                parser.OFPActionOutput(dst_port),
            ]
            match_fwd = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_real,
                ipv4_dst=dst_real,
                in_port=in_port,
                **forward_l4_match,
            )
            # ── Reverse at client's edge: match(src=server_vip, dst=client_vip) → DNAT dst→src_real ─
            # edge9 already SNATs server_real→server_vip before forwarding up.
            src_port = self._port_toward_host(dpid, src_real)
            if src_port is None:
                src_port = self.mac_to_port.get(dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)
            actions_rev = [
                parser.OFPActionSetField(ipv4_dst=src_real),
                parser.OFPActionSetField(eth_dst=src_real_mac),
                parser.OFPActionOutput(src_port),
            ]
            match_rev = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=dst_vip,
                ipv4_dst=src_vip,
                **reverse_l4_match,
            )

            self._send_packet_out(msg, dp, in_port, actions_fwd)
            cookie_fwd = 0 if is_icmp_err else self._vip_cookie(src_vip)
            cookie_rev = 0 if is_icmp_err else self._vip_cookie(dst_vip)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                           actions=actions_fwd, cookie=cookie_fwd, idle_timeout=5)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                           actions=actions_rev, cookie=cookie_rev, idle_timeout=5)
            self._install_real_to_real_last_hop(
                forward_l4_match, reverse_l4_match,
                src_vip, dst_vip, src_vip_mac, dst_vip_mac,
                src_real_mac, dst_real_mac, dst_real, is_icmp_err,
            )
            self.logger.debug("REAL-TO-REAL: cross-switch %s->%s fabric=%s->%s",
                              src_real, dst_real, src_vip, dst_vip)
        else:
            # ── Same-switch: full SNAT+DNAT in one place ─────────────────────────────
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=src_vip),
                parser.OFPActionSetField(ipv4_dst=dst_real),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_real_mac),
                parser.OFPActionOutput(dst_port),
            ]
            match_fwd = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_real,
                ipv4_dst=dst_real,
                in_port=in_port,
                **forward_l4_match,
            )
            self._send_packet_out(msg, dp, in_port, actions_fwd)
            cookie_fwd = 0 if is_icmp_err else self._vip_cookie(src_vip)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                           actions=actions_fwd, cookie=cookie_fwd, idle_timeout=5)

            # Reverse: dst_real replies to src_vip → SNAT+DNAT back to src_real
            if src_real_mac and dst_vip_mac:
                src_port = self._port_toward_host(dpid, src_real)
                if src_port is None:
                    src_port = self.mac_to_port.get(dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)
                actions_rev = [
                    parser.OFPActionSetField(ipv4_src=dst_vip),
                    parser.OFPActionSetField(ipv4_dst=src_real),
                    parser.OFPActionSetField(eth_src=dst_vip_mac),
                    parser.OFPActionSetField(eth_dst=src_real_mac),
                    parser.OFPActionOutput(src_port),
                ]
                match_rev = parser.OFPMatch(
                    eth_type=0x0800,
                    ipv4_src=dst_real,
                    ipv4_dst=src_vip,
                    **reverse_l4_match,
                )
                cookie_rev = 0 if is_icmp_err else self._vip_cookie(dst_vip)
                self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                               actions=actions_rev, cookie=cookie_rev, idle_timeout=5)
            self.logger.debug("REAL-TO-REAL: same-switch %s->%s (VIPs: %s->%s)",
                              src_real, dst_real, src_vip, dst_real)

    def _handle_real_to_vip(self, msg, dp, pkt, ip4, eth, in_port, dpid, src_real, dst_vip, rppt_start=None):
        """
        Handle traffic from real host to VIP: SNAT + DNAT.
        Simplified version - no complex session tracking.
        """
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        # Get source VIP for SNAT
        src_vip = self.primary_vip.get(src_real)
        if not src_vip:
            self.logger.warning("REAL-TO-VIP: No VIP for source %s", src_real)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        # Get real destination
        real_dst = self.vip_owner.get(dst_vip)
        if not real_dst:
            self.logger.warning("REAL-TO-VIP: No owner for VIP %s", dst_vip)
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        src_vip_mac = self._ensure_vip_mac(src_vip)
        dst_vip_mac = self._ensure_vip_mac(dst_vip)
        dst_real_mac = self.host_ip_to_mac.get(real_dst)
        
        if not src_vip_mac or not dst_real_mac:
            self.logger.debug("REAL-TO-VIP: Missing MACs, flooding")
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        # Try to handle as L4 session first (TCP/UDP).
        # _handle_l4_session returns True  → session flows installed, we're done.
        # _handle_l4_session returns False → either not TCP/UDP (ICMP etc), OR it's TCP/UDP
        #   but was deliberately blocked (e.g. late stray packet to a GRACE VIP).
        #   In the blocked case we must NOT fall through to the generic flow installer.
        is_l4 = (pkt.get_protocol(udp.udp) is not None or
                 pkt.get_protocol(tcp.tcp) is not None)
        if self._handle_l4_session(msg, dp, pkt, in_port, src_real, dst_vip, rppt_start):
            return
        if is_l4:
            # TCP/UDP packet blocked by session guard — drop, no generic flows
            return

        # Only install flows for non-TCP/UDP traffic (e.g., ICMP/ping).
        # Use the last-hop architecture (mirrors _handle_real_to_real): the
        # server's real IP must NEVER appear in nw_dst/nw_src on inter-switch
        # links.  For cross-switch sessions, nw_dst stays the server VIP across
        # the fabric and the DNAT/SNAT to the real IP happens only at the
        # server's edge switch.  Doing one-shot SNAT+DNAT at the ingress edge
        # (the previous behaviour) leaked real_dst onto every fabric link in
        # both directions.
        forward_l4_match, reverse_l4_match = self._extract_l4_match_fields(pkt, ip4)
        is_icmp_err = self._is_icmp_error(pkt)

        src_real_mac = self.host_ip_to_mac.get(src_real) or eth.src
        if src_real_mac:
            self.host_ip_to_mac[src_real] = src_real_mac

        # Route toward the destination's edge by topology (no transit flooding).
        dst_port = self._port_toward_host(dpid, real_dst)
        if dst_port is None:
            dst_port = self.mac_to_port.get(dpid, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
        is_cross_switch = not self._is_final_delivery(dpid, real_dst, dst_port)

        if is_cross_switch:
            # ── Forward at client's edge: SNAT src→src_vip, keep dst=dst_vip ──
            # nw_dst stays the server VIP; the real-IP rewrite is deferred to
            # the server's edge switch via _install_real_to_real_last_hop.
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=src_vip),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_vip_mac),
                parser.OFPActionOutput(dst_port),
            ]
            match_fwd = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_real,
                ipv4_dst=dst_vip,
                in_port=in_port,
                **forward_l4_match,
            )
            # ── Reverse at client's edge: match(src=dst_vip, dst=src_vip) → DNAT dst→src_real ──
            # The server's edge SNATs real_dst→dst_vip before the reply crosses the fabric.
            src_port = self._port_toward_host(dpid, src_real)
            if src_port is None:
                src_port = self.mac_to_port.get(dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)
            actions_rev = [
                parser.OFPActionSetField(ipv4_dst=src_real),
                parser.OFPActionSetField(eth_dst=src_real_mac),
                parser.OFPActionOutput(src_port),
            ]
            match_rev = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=dst_vip,
                ipv4_dst=src_vip,
                **reverse_l4_match,
            )

            self._send_packet_out(msg, dp, in_port, actions_fwd)
            cookie_fwd = 0 if is_icmp_err else self._vip_cookie(src_vip)
            cookie_rev = 0 if is_icmp_err else self._vip_cookie(dst_vip)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                           actions=actions_fwd, cookie=cookie_fwd, idle_timeout=5)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                           actions=actions_rev, cookie=cookie_rev, idle_timeout=5)
            # Install the DNAT(fwd)/SNAT(rev) flows at the server's edge switch.
            self._install_real_to_real_last_hop(
                forward_l4_match, reverse_l4_match,
                src_vip, dst_vip, src_vip_mac, dst_vip_mac,
                src_real_mac, dst_real_mac, real_dst, is_icmp_err,
            )
            self.logger.debug("REAL-TO-VIP: cross-switch ICMP %s->%s fabric=%s->%s",
                              src_real, dst_vip, src_vip, dst_vip)
        else:
            # ── Same-switch: full SNAT+DNAT here; the real IP never leaves this
            # switch (it only ever goes out the destination host's leaf port).
            actions_fwd = [
                parser.OFPActionSetField(ipv4_src=src_vip),
                parser.OFPActionSetField(ipv4_dst=real_dst),
                parser.OFPActionSetField(eth_src=src_vip_mac),
                parser.OFPActionSetField(eth_dst=dst_real_mac),
                parser.OFPActionOutput(dst_port),
            ]
            match_fwd = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_real,
                ipv4_dst=dst_vip,
                in_port=in_port,
                **forward_l4_match,
            )
            self._send_packet_out(msg, dp, in_port, actions_fwd)
            cookie_fwd = 0 if is_icmp_err else self._vip_cookie(src_vip)
            self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_fwd,
                           actions=actions_fwd, cookie=cookie_fwd, idle_timeout=5)

            # Reverse: real_dst replies to src_vip → SNAT src→dst_vip, DNAT dst→src_real.
            if src_real_mac and dst_vip_mac:
                src_port = self._port_toward_host(dpid, src_real)
                if src_port is None:
                    src_port = self.mac_to_port.get(dpid, {}).get(src_real_mac, ofp.OFPP_FLOOD)
                actions_rev = [
                    parser.OFPActionSetField(ipv4_src=dst_vip),
                    parser.OFPActionSetField(ipv4_dst=src_real),
                    parser.OFPActionSetField(eth_src=dst_vip_mac),
                    parser.OFPActionSetField(eth_dst=src_real_mac),
                    parser.OFPActionOutput(src_port),
                ]
                match_rev = parser.OFPMatch(
                    eth_type=0x0800,
                    ipv4_src=real_dst,
                    ipv4_dst=src_vip,
                    **reverse_l4_match,
                )
                cookie_rev = 0 if is_icmp_err else self._vip_cookie(dst_vip)
                self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match_rev,
                               actions=actions_rev, cookie=cookie_rev, idle_timeout=5)
            self.logger.debug("REAL-TO-VIP: same-switch ICMP %s->%s (VIPs: %s->%s)",
                              src_real, dst_vip, src_vip, real_dst)

    def _handle_vip_to_real(self, msg, dp, pkt, ip4, eth, in_port, dpid, src_vip, dst_real):
        """
        Handle traffic from VIP to real host: reverse SNAT.
        This is the reply path.
        """
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        if src_vip not in self.vip_owner:
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        dst_real_mac = self.host_ip_to_mac.get(dst_real)
        if not dst_real_mac:
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        l4_info = self._extract_l4_info(pkt, ip4)
        # Route toward the destination's edge by topology — transit switches must
        # NOT flood (they never learn synthetic VIP MACs). Flood only as fallback.
        dst_port = self._port_toward_host(dpid, dst_real)
        if dst_port is None:
            dst_port = self.mac_to_port.get(dpid, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
        is_final = self._is_final_delivery(dpid, dst_real, dst_port)
        fwd_eth_dst = dst_real_mac if is_final else (
            self._ensure_vip_mac(self.primary_vip.get(dst_real)) or dst_real_mac
        )
        actions = [
            parser.OFPActionSetField(eth_dst=fwd_eth_dst),
            parser.OFPActionOutput(dst_port),
        ]

        self._send_packet_out(msg, dp, in_port, actions)

        if not is_final:
            # Intermediate switch — a packet with nw_dst=real_ip should never appear
            # here in the last-hop architecture.  Forward the stray packet so the TCP
            # connection survives but do NOT install a persistent flow: a flow with
            # nw_dst=server_real in its match would expose the real IP in the flow
            # table of an intermediate switch.  The correct last-hop flows will be
            # installed by _install_last_hop_flows; once they are in place the traffic
            # goes through the VIP-only path and this handler stops being called.
            self.logger.warning(
                "VIP-TO-REAL: stray packet src=%s dst=%s on intermediate dp=%016x "
                "(not final-delivery) — forwarding without flow install",
                src_vip, dst_real, dpid,
            )
            return

        # Final-delivery switch: install a flow so subsequent packets don't hit the
        # controller.  For L4 use the session-matched fields; for ICMP use generic.
        forward_l4_match, _ = self._extract_l4_match_fields(pkt, ip4)
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_vip,
            ipv4_dst=dst_real,
            in_port=in_port,
            **forward_l4_match,
        )
        cookie = 0 if (l4_info is None and self._is_icmp_error(pkt)) else self._vip_cookie(src_vip)
        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match,
                       actions=actions, cookie=cookie, idle_timeout=5)
        self.logger.debug("VIP-TO-REAL: installed final-delivery flow on dp=%016x for %s->%s",
                          dpid, src_vip, dst_real)

    def _handle_vip_to_vip(self, msg, dp, pkt, ip4, eth, in_port, dpid, src_vip, dst_vip, rppt_start=None):
        """Handle VIP-to-VIP traffic on intermediate and last-hop switches.

        Option 1 (host-side VIP): hosts hold their primary VIP as data-plane IP.
        - Last-hop switch: rewrite eth_dst to real host MAC, output:host_port.
          No ipv4_dst rewrite — the host accepts the VIP natively.
        - Intermediate switch: update eth_dst for next-hop routing, VIPs unchanged.
        For TCP/UDP: maintain session state, pin VIPs, and log RPPT_MEASURED so
        that VIP lifecycle management and benchmark metrics work correctly.
        """
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        real_dst = self.vip_owner.get(dst_vip)
        if not real_dst:
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        dst_real_mac = self.host_ip_to_mac.get(real_dst)
        if not dst_real_mac:
            self._forward_packet(msg, dp, in_port, dpid, eth.dst, ofp.OFPP_FLOOD)
            return

        # Route toward the destination's edge by topology — transit (VIP->VIP)
        # switches must NOT flood (they never learn synthetic VIP MACs).
        dst_port = self._port_toward_host(dpid, real_dst)
        if dst_port is None:
            dst_port = self.mac_to_port.get(dpid, {}).get(dst_real_mac, ofp.OFPP_FLOOD)
        forward_l4_match, _ = self._extract_l4_match_fields(pkt, ip4)

        # Session bookkeeping for TCP/UDP — must happen before flow install so we
        # can use the session state to pick the correct idle_timeout.
        l4_info = self._extract_l4_info(pkt, ip4)
        sess = None
        is_new_session = False
        if l4_info is not None:
            proto = int(l4_info.get("proto", 0))
            src_real = self.vip_owner.get(src_vip)
            if src_real:
                now = time()
                key = self._session_key(src_real, dst_vip, proto,
                                        int(l4_info["src_port"]), int(l4_info["dst_port"]))
                sess = self.session_table.get(key)
                is_new_session = sess is None
                if is_new_session:
                    is_udp = proto == socket.IPPROTO_UDP
                    sess = {
                        "client_real_ip": src_real,
                        "server_vip": dst_vip,
                        "proto": proto,
                        "client_port": int(l4_info["src_port"]),
                        "server_port": int(l4_info["dst_port"]),
                        "client_src_vip": src_vip,
                        "server_real_ip": real_dst,
                        "server_reply_vip": dst_vip,
                        "state": self.SESSION_UDP_ACTIVE if is_udp else self.SESSION_TCP_SYN_SEEN,
                        "expires_at": now + (self.UDP_ACTIVE_TIMEOUT if is_udp else self.TCP_SYN_SEEN_TIMEOUT),
                    }
                    self.session_table[key] = sess
                    self._pin_vip_session(src_vip)
                    self._pin_vip_session(dst_vip)

                if proto == socket.IPPROTO_TCP:
                    tcp_pkt = pkt.get_protocol(tcp.tcp)
                    self._update_tcp_session_state(sess, tcp_pkt, now)
                elif proto == socket.IPPROTO_UDP:
                    sess["state"] = self.SESSION_UDP_ACTIVE
                    sess["expires_at"] = now + self.UDP_ACTIVE_TIMEOUT

                self._touch_vip(src_vip, now)
                self._touch_vip(dst_vip, now)

        # Use _find_edge_switch_for_host so the last-hop decision is topology-aware
        # even without EventLinkAdd.  Only DNAT at the switch that directly hosts real_dst.
        edge_loc = self._find_edge_switch_for_host(real_dst)
        is_last_hop = (edge_loc is not None) and (edge_loc[0] == dpid)

        if is_last_hop:
            # Option 1: host has dst_vip as its own IP — deliver by MAC only,
            # no ipv4_dst rewrite needed (host accepts the VIP natively).
            actions = [
                parser.OFPActionSetField(eth_dst=dst_real_mac),
                parser.OFPActionOutput(dst_port),
            ]
        else:
            # Intermediate switch: route toward real_dst without rewriting IP.
            # VIPs stay in nw_src/nw_dst all the way to the destination edge switch.
            actions = [
                parser.OFPActionSetField(eth_dst=self._ensure_vip_mac(dst_vip)),
                parser.OFPActionOutput(dst_port),
            ]

        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=src_vip,
            ipv4_dst=dst_vip,
            in_port=in_port,
            **forward_l4_match,
        )

        idle_timeout = self._flow_idle_timeout_for_session(sess) if sess else self.TCP_ESTABLISHED_TIMEOUT
        self._send_packet_out(msg, dp, in_port, actions)
        cookie = 0 if self._is_icmp_error(pkt) else self._vip_cookie(dst_vip)
        self._add_flow(dp, priority=self.FLOW_PRIORITY_VIP, match=match, actions=actions,
                       cookie=cookie, idle_timeout=idle_timeout)

        if is_new_session and rppt_start is not None:
            elapsed_ms = (time() - rppt_start) * 1000
            rppt_key = (l4_info.get("proto"), src_vip, dst_vip,
                        l4_info.get("src_port"), l4_info.get("dst_port"))
            self.logger.info("RPPT_MEASURED: key=%s elapsed_ms=%.3f", rppt_key, elapsed_ms)

    def _is_icmp_error(self, pkt) -> bool:
        """Check if ICMP packet is an error message (not echo request/reply)."""
        icmp_pkt = pkt.get_protocol(icmp.icmp)
        if not icmp_pkt:
            return False
        # ICMP error messages: Destination Unreachable (3), Time Exceeded (11), 
        # Parameter Problem (12), Source Quench (4 - deprecated)
        # Echo Request (8) and Echo Reply (0) are NOT errors
        return icmp_pkt.type not in (icmp.ICMP_ECHO_REQUEST, icmp.ICMP_ECHO_REPLY)

    def _extract_l4_match_fields(self, pkt, ip4):
        """Build protocol-aware OpenFlow match fields for forward and reverse directions."""
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        if tcp_pkt:
            return (
                {"ip_proto": socket.IPPROTO_TCP, "tcp_src": tcp_pkt.src_port, "tcp_dst": tcp_pkt.dst_port},
                {"ip_proto": socket.IPPROTO_TCP, "tcp_src": tcp_pkt.dst_port, "tcp_dst": tcp_pkt.src_port},
            )

        udp_pkt = pkt.get_protocol(udp.udp)
        if udp_pkt:
            return (
                {"ip_proto": socket.IPPROTO_UDP, "udp_src": udp_pkt.src_port, "udp_dst": udp_pkt.dst_port},
                {"ip_proto": socket.IPPROTO_UDP, "udp_src": udp_pkt.dst_port, "udp_dst": udp_pkt.src_port},
            )

        icmp_pkt = pkt.get_protocol(icmp.icmp)
        if icmp_pkt:
            reverse_type = icmp_pkt.type
            if icmp_pkt.type == icmp.ICMP_ECHO_REQUEST:
                reverse_type = icmp.ICMP_ECHO_REPLY
            elif icmp_pkt.type == icmp.ICMP_ECHO_REPLY:
                reverse_type = icmp.ICMP_ECHO_REQUEST

            return (
                {
                    "ip_proto": socket.IPPROTO_ICMP,
                    "icmpv4_type": icmp_pkt.type,
                    "icmpv4_code": icmp_pkt.code,
                },
                {
                    "ip_proto": socket.IPPROTO_ICMP,
                    "icmpv4_type": reverse_type,
                    "icmpv4_code": icmp_pkt.code,
                },
            )

        return ({"ip_proto": ip4.proto}, {"ip_proto": ip4.proto})

    def _ensure_vip_mac(self, vip: str) -> Optional[str]:
        """Return VIP MAC, generating and caching one if missing."""
        if not vip:
            return None
        vip_mac = self.vip_mac_map.get(vip)
        if vip_mac:
            return vip_mac
        vip_mac = self._generate_vip_mac(vip)
        self.vip_mac_map[vip] = vip_mac
        self.logger.warning("VIP_MAC: generated missing MAC mapping for VIP %s -> %s", vip, vip_mac)
        return vip_mac

    def _forward_packet(self, msg, dp, in_port, dpid, dst_mac, out_port):
        """Forward packet without modification."""
        parser = dp.ofproto_parser
        actions = [parser.OFPActionOutput(out_port)]
        self._send_packet_out(msg, dp, in_port, actions)

    def _send_packet_out(self, msg, dp, in_port, actions):
        """Send packet-out message."""
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        data = msg.data if msg.buffer_id == ofp.OFP_NO_BUFFER else None
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data,
        )
        dp.send_msg(out)

    def _release_first_packet(self, msg, dp, in_port):
        """Re-inject the reactive first packet through the pipeline AFTER its flow
        is installed, so it follows the same hardware path — and the same
        ordering — as packets 1,2,3…  A BarrierRequest guarantees the preceding
        FlowMod is fully applied before the switch processes this packet-out.

        Why this matters: previously we released packet 0 via the controller
        path (actions inline) and THEN installed the flow.  The moment the flow
        landed, the following packets took the fast hardware path and overtook
        packet 0.  That single reorder breaks iperf2's UDP stream init (it keys
        all accounting off an in-order first datagram → 0-byte / -nan display)
        even though every datagram is delivered.  Sending the packet through
        OFPP_TABLE means it's rewritten by the just-installed VIP flow exactly
        like the rest, so obfuscation is unchanged."""
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        dp.send_msg(parser.OFPBarrierRequest(dp))
        data = msg.data if msg.buffer_id == ofp.OFP_NO_BUFFER else None
        dp.send_msg(parser.OFPPacketOut(
            datapath=dp,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=[parser.OFPActionOutput(ofp.OFPP_TABLE)],
            data=data,
        ))

