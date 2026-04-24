# baseline_controller.py

import socket
import ipaddress
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


class BaselineController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(BaselineController, self).__init__(*args, **kwargs)

        self.datapaths: Dict[int, object] = {}

        # adjacency[src_dpid][dst_dpid] = out_port
        self.adjacency: Dict[int, Dict[int, int]] = {}

        # MAC/IP learning
        self.mac_to_loc: Dict[str, Tuple[int, int]] = {}     # mac -> (dpid, port)
        self.ip_to_mac: Dict[str, str] = {}                  # ip -> mac
        self.mac_to_ip: Dict[str, str] = {}                  # mac -> ip

        # switch ports per dpid
        self.switch_ports: Dict[int, Set[int]] = {}

        self.topology_thread = hub.spawn(self._topology_loop)

    # =========================================================
    # OpenFlow setup
    # =========================================================
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        self.datapaths[dp.id] = dp
        self.adjacency.setdefault(dp.id, {})
        self.switch_ports.setdefault(dp.id, set())

        # table-miss -> controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions)

        self.logger.info("Switch connected: dpid=%s", dp.id)

    def add_flow(self, dp, priority, match, actions, idle_timeout=60, hard_timeout=0):
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(
            datapath=dp,
            priority=priority,
            match=match,
            instructions=inst,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout
        )
        dp.send_msg(mod)

    # =========================================================
    # Topology discovery
    # =========================================================
    def _topology_loop(self):
        while True:
            try:
                self._update_topology()
            except Exception as e:
                self.logger.warning("Topology update error: %s", e)
            hub.sleep(2)

    def _update_topology(self):
        switch_list = get_switch(self, None)
        link_list = get_link(self, None)

        current_switches: Set[int] = set()

        for sw in switch_list:
            dpid = sw.dp.id
            current_switches.add(dpid)
            self.datapaths[dpid] = sw.dp
            self.adjacency.setdefault(dpid, {})
            self.switch_ports.setdefault(dpid, set())

            ports = set()
            for p in sw.ports:
                if p.port_no <= ofproto_v1_3.OFPP_MAX:
                    ports.add(p.port_no)
            self.switch_ports[dpid] = ports

        # clear switch-to-switch adjacency and rebuild
        for dpid in list(self.adjacency.keys()):
            self.adjacency[dpid] = {}

        for link in link_list:
            src = link.src
            dst = link.dst
            self.adjacency.setdefault(src.dpid, {})
            self.adjacency.setdefault(dst.dpid, {})
            self.adjacency[src.dpid][dst.dpid] = src.port_no

    @set_ev_cls(topo_event.EventSwitchEnter)
    @set_ev_cls(topo_event.EventSwitchLeave)
    @set_ev_cls(topo_event.EventLinkAdd)
    @set_ev_cls(topo_event.EventLinkDelete)
    def topology_change_handler(self, ev):
        self._update_topology()

    # =========================================================
    # BFS path
    # =========================================================
    def get_path(self, src: int, dst: int) -> Optional[List[int]]:
        if src == dst:
            return [src]

        visited: Set[int] = set()
        queue: List[Tuple[int, List[int]]] = [(src, [src])]

        while queue:
            node, path = queue.pop(0)

            if node in visited:
                continue
            visited.add(node)

            for neighbor in self.adjacency.get(node, {}):
                if neighbor == dst:
                    return path + [neighbor]
                if neighbor not in visited:
                    queue.append((neighbor, path + [neighbor]))

        return None

    # =========================================================
    # Helpers
    # =========================================================
    def is_host_port(self, dpid: int, port_no: int) -> bool:
        # if a port is not used as a switch-to-switch link, treat it as a host-facing port
        for neighbor in self.adjacency.get(dpid, {}):
            if self.adjacency[dpid][neighbor] == port_no:
                return False
        return True

    def learn_host(self, dpid: int, port_no: int, mac: str, ip: Optional[str] = None):
        self.mac_to_loc[mac] = (dpid, port_no)
        if ip:
            self.ip_to_mac[ip] = mac
            self.mac_to_ip[mac] = ip

    def flood(self, dp, in_port: int, data: bytes):
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofp.OFP_NO_BUFFER,
            in_port=in_port,
            actions=actions,
            data=data
        )
        dp.send_msg(out)

    def packet_out(self, dp, in_port: int, out_port: int, data: bytes):
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        actions = [parser.OFPActionOutput(out_port)]
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofp.OFP_NO_BUFFER,
            in_port=in_port,
            actions=actions,
            data=data
        )
        dp.send_msg(out)

    def install_path(self, src_ip: str, dst_ip: str,
                     src_dpid: int, src_in_port: int,
                     dst_dpid: int, dst_out_port: int):
        path = self.get_path(src_dpid, dst_dpid)
        if not path:
            self.logger.warning("No path from %s to %s", src_dpid, dst_dpid)
            return

        # forward direction
        for i, sw in enumerate(path):
            dp = self.datapaths.get(sw)
            if not dp:
                continue
            parser = dp.ofproto_parser

            if len(path) == 1:
                out_port = dst_out_port
            elif i == len(path) - 1:
                out_port = dst_out_port
            else:
                next_sw = path[i + 1]
                out_port = self.adjacency[sw][next_sw]

            match = parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=src_ip,
                ipv4_dst=dst_ip
            )
            actions = [parser.OFPActionOutput(out_port)]
            self.add_flow(dp, 100, match, actions, idle_timeout=120)

        # reverse direction if source host known
        src_mac = self.ip_to_mac.get(src_ip)
        if src_mac and src_mac in self.mac_to_loc:
            rev_dst_dpid, rev_dst_port = self.mac_to_loc[src_mac]
            rev_path = self.get_path(dst_dpid, rev_dst_dpid)

            if rev_path:
                for i, sw in enumerate(rev_path):
                    dp = self.datapaths.get(sw)
                    if not dp:
                        continue
                    parser = dp.ofproto_parser

                    if len(rev_path) == 1:
                        out_port = rev_dst_port
                    elif i == len(rev_path) - 1:
                        out_port = rev_dst_port
                    else:
                        next_sw = rev_path[i + 1]
                        out_port = self.adjacency[sw][next_sw]

                    match = parser.OFPMatch(
                        eth_type=0x0800,
                        ipv4_src=dst_ip,
                        ipv4_dst=src_ip
                    )
                    actions = [parser.OFPActionOutput(out_port)]
                    self.add_flow(dp, 100, match, actions, idle_timeout=120)

    def forward_current_packet(self, src_dpid: int, dst_dpid: int,
                               src_in_port: int, dst_out_port: int,
                               data: bytes):
        path = self.get_path(src_dpid, dst_dpid)
        if not path:
            return

        dp = self.datapaths[src_dpid]

        if len(path) == 1:
            out_port = dst_out_port
        else:
            next_sw = path[1]
            out_port = self.adjacency[src_dpid][next_sw]

        self.packet_out(dp, src_in_port, out_port, data)

    # =========================================================
    # PacketIn
    # =========================================================
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        dpid = dp.id
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return

        # Ignore LLDP
        if eth.ethertype == 0x88cc:
            return

        src_mac = eth.src
        self.learn_host(dpid, in_port, src_mac)

        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self.handle_arp(dp, in_port, msg.data, src_mac, arp_pkt)
            return

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.handle_ipv4(dp, in_port, msg.data, src_mac, ip_pkt)
            return

    # =========================================================
    # ARP
    # =========================================================
    def handle_arp(self, dp, in_port: int, data: bytes, src_mac: str, arp_pkt):
        dpid = dp.id

        src_ip = arp_pkt.src_ip
        dst_ip = arp_pkt.dst_ip

        self.learn_host(dpid, in_port, src_mac, src_ip)

        # If we know destination host location, send ARP directly toward it
        dst_mac = self.ip_to_mac.get(dst_ip)
        if dst_mac and dst_mac in self.mac_to_loc:
            dst_dpid, dst_port = self.mac_to_loc[dst_mac]
            self.forward_current_packet(dpid, dst_dpid, in_port, dst_port, data)
            return

        # Otherwise flood
        self.flood(dp, in_port, data)

    # =========================================================
    # IPv4
    # =========================================================
    def handle_ipv4(self, dp, in_port: int, data: bytes, src_mac: str, ip_pkt):
        dpid = dp.id

        src_ip = ip_pkt.src
        dst_ip = ip_pkt.dst

        self.learn_host(dpid, in_port, src_mac, src_ip)

        dst_mac = self.ip_to_mac.get(dst_ip)
        if not dst_mac:
            self.flood(dp, in_port, data)
            return

        if dst_mac not in self.mac_to_loc:
            self.flood(dp, in_port, data)
            return

        dst_dpid, dst_port = self.mac_to_loc[dst_mac]

        self.install_path(src_ip, dst_ip, dpid, in_port, dst_dpid, dst_port)
        self.forward_current_packet(dpid, dst_dpid, in_port, dst_port, data)
