#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
# campus_topo.py — University-campus MTD topology (flat L3, loop-free)
#
# A more realistic "industry" topology than industrytp.py's uniform fat-tree:
# it models a university campus as a 3-tier hierarchy with named zones of
# DIFFERENT density (dorms are dense, the data center is sparse, etc.) and a
# varied fan-out per building — the way a real campus network actually looks.
#
#   Core  (core1)            campus backbone
#     └── Building distribution switches (agg1..aggK)   — one per building
#           └── Access switches (edge1..edgeM)          — wiring closets
#                 └── Hosts (h1..hN)                     — end devices
#
# DESIGN CONSTRAINTS (must hold for the controller + benchmark_nsdi.sh):
#   * Single flat 10.0.0.0/8 space. Real hosts stay BELOW 10.0.3.1 (the
#     controller's VIP_POOL_START); VIPs live at/above it.
#   * Hosts are named h1..hN CONTIGUOUSLY (the benchmark stops at the first
#     gap) and each gets a 10.x address.
#   * Switches are OVS bridges; leaves are edge*, spines are agg*/core*
#     (the benchmark's NRT link-failure test keys off these names).
#   * LOOP-FREE TREE ONLY. The controller has no spanning-tree/loop handling,
#     so redundant uplinks would cause broadcast storms. Each edge has exactly
#     one uplink (to its building agg); each agg has exactly one uplink (to
#     core). "VLANs" are modelled as logical zones, NOT 802.1Q segments.
#
# "VLAN"/zone realism is logical: hosts are grouped into zones (dorm, faculty,
# lab, server) with realistic per-zone density, and the grouping is reported,
# but addressing stays flat so the existing flat-L3 controller works unchanged.
#
# Usage:
#   sudo python campus_topo.py small     # ~16  hosts
#   sudo python campus_topo.py medium    # ~100 hosts   (default)
#   sudo python campus_topo.py large     # ~500 hosts (enterprise)
# =============================================================================

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel
import sys
import json
import os

NS_MAP_FILE = '/tmp/mininet_ns_map.json'
EXPECT_FILE = '/tmp/mtd_topology_expectations.json'

# =============================================================================
# Campus plans.
#
# Each plan is a list of buildings.  A building is:
#   (building_name, zone, [hosts_on_access_sw_1, hosts_on_access_sw_2, ...])
# Every inner number creates one access (edge) switch with that many hosts.
# Host counts are intentionally uneven to mimic real density differences:
#   dorm    — dense student housing
#   faculty — offices / classrooms (academic + admin + library)
#   lab     — research / teaching labs
#   server  — data-center / EHR-style server farm (sparse, high-value)
# Totals: small=16, medium=100, large=500 (enterprise; dense edges, ~medium fabric).
# =============================================================================
CAMPUS_PLANS = {
    'small': [
        ('ResidenceHall', 'dorm',    [4, 4]),
        ('ScienceBldg',   'faculty', [3, 3]),
        ('DataCenter',    'server',  [2]),
    ],
    'medium': [
        ('ResidenceHallA', 'dorm',    [12, 12, 12]),
        ('ResidenceHallB', 'dorm',    [12, 12]),
        ('Engineering',    'lab',     [8, 8]),
        ('Library',        'faculty', [10]),
        ('AdminBldg',      'faculty', [6]),
        ('DataCenter',     'server',  [4, 4]),
    ],
    # ENTERPRISE scale via host DENSITY, not fabric width: many hosts per edge,
    # while the switch/link count stays near 'medium' (which discovers cleanly).
    # The old 39-switch/76-link 'large' stalled LLDP discovery and starved the
    # bootstrap thread ("ROTATE: 0 hosts"); keeping ~19 switches avoids that
    # while reaching enterprise host counts.
    # 500 hosts, 6 agg + 12 edge + 1 core = 19 switches, 36 directed links.
    # Real-host budget is <762 (10.0.0.1..10.0.2.254, below VIP_POOL_START).
    'large': [
        ('ResidenceHallA', 'dorm',    [50, 50]),
        ('ResidenceHallB', 'dorm',    [50, 50]),
        ('Engineering',    'lab',     [48, 48]),
        ('Library',        'faculty', [42, 42]),
        ('BusinessSchool', 'faculty', [40, 40]),
        ('DataCenter',     'server',  [20, 20]),
    ],
}


class CampusTopo(Topo):
    """University-campus hierarchy: core -> building agg -> access edge -> hosts."""

    def __init__(self, size='medium', **opts):
        if size not in CAMPUS_PLANS:
            raise ValueError("size must be small|medium|large, got %r" % size)
        self.size = size
        self.plan = CAMPUS_PLANS[size]
        # Records for the runner's summary / ns_map: hN -> (zone, building, edge)
        self.host_zone = {}
        super(CampusTopo, self).__init__(**opts)

    def build(self):
        # ── Core backbone ───────────────────────────────────────────────────
        core = self.addSwitch('core1', cls=OVSKernelSwitch, dpid='%016x' % 1)

        host_id = 1
        agg_idx = 0
        edge_idx = 0

        for (building, zone, access_hosts) in self.plan:
            # ── Building distribution switch (one per building) ──────────────
            agg_idx += 1
            agg = self.addSwitch('agg%d' % agg_idx, cls=OVSKernelSwitch,
                                 dpid='%016x' % (100 + agg_idx))
            # Uplink to core FIRST so the agg's port 1 is its core uplink.
            self.addLink(agg, core)

            # ── Access (wiring-closet) switches in this building ─────────────
            for nhosts in access_hosts:
                edge_idx += 1
                edge = self.addSwitch('edge%d' % edge_idx, cls=OVSKernelSwitch,
                                      dpid='%016x' % (200 + edge_idx))
                # Uplink to building agg FIRST so edge port 1 is its uplink.
                self.addLink(edge, agg)

                # ── Hosts on this access switch ──────────────────────────────
                for _ in range(nhosts):
                    # Flat sequential addressing, kept below 10.0.3.1:
                    #   h1..h254   -> 10.0.0.1-254
                    #   h255..h508 -> 10.0.1.1-254  (etc.)
                    o3 = (host_id - 1) // 254
                    o4 = ((host_id - 1) % 254) + 1
                    ip = '10.0.%d.%d/8' % (o3, o4)
                    mac = '00:00:00:00:%02x:%02x' % (host_id // 256, host_id % 256)
                    hname = 'h%d' % host_id
                    self.addHost(hname, ip=ip, mac=mac, defaultRoute='')
                    self.addLink(hname, edge)
                    self.host_zone[hname] = (zone, building, 'edge%d' % edge_idx)
                    host_id += 1

        # Counts for the runner.
        self.n_core = 1
        self.n_agg = agg_idx
        self.n_edge = edge_idx
        self.n_hosts = host_id - 1


def run(size):
    topo = CampusTopo(size=size)

    # ── Clear stale per-run state from any previous controller/topology ──────
    # /tmp/mtd_vip_mapping.json is global shared state read by the benchmark.
    # A stale one (e.g. left by an earlier MTD run) makes a baseline run target
    # VIPs that don't exist, or makes a new run target the wrong VIPs. Removing
    # it here means it's absent until the MTD controller repopulates it (the
    # benchmark then correctly falls back to real IPs for non-MTD controllers).
    for stale in ('/tmp/mtd_vip_mapping.json',):
        try:
            os.remove(stale)
            print('[clean] removed stale %s' % stale)
        except OSError:
            pass

    # ── Write topology expectations BEFORE starting the network ──────────────
    # These counts come from the plan, not the running network, so writing them
    # first means the file exists before any switch connects — the controller
    # then never completes discovery against stale hardcoded defaults.
    # Switch<->switch links only (hosts are not LLDP-discovered links).
    n_sw = topo.n_core + topo.n_agg + topo.n_edge
    n_undirected = topo.n_agg + topo.n_edge          # core-agg + agg-edge links
    n_directed = n_undirected * 2
    expectations = {
        'expected_switches': n_sw,
        'expected_directed_links': n_directed,
        'expected_hosts': 0,
    }
    try:
        with open(EXPECT_FILE, 'w') as f:
            json.dump(expectations, f)
        print('[expect] switches=%d directed_links=%d -> %s'
              % (n_sw, n_directed, EXPECT_FILE))
    except Exception as e:
        print('[expect] WARNING: could not write %s: %s' % (EXPECT_FILE, e))

    net = Mininet(topo=topo, controller=RemoteController, switch=OVSKernelSwitch)
    net.start()

    # ── Write host namespace map for the controller (Option-1 VIP push) ──────
    # Needs the running network (host PIDs), so it comes after net.start().
    ns_map = {}
    for host in net.hosts:
        real_ip = host.IP()
        if not real_ip:
            continue
        ns_map[real_ip] = {
            'pid':   host.pid,
            'iface': host.defaultIntf().name,
            'mac':   host.MAC(),
        }
    try:
        with open(NS_MAP_FILE, 'w') as f:
            json.dump(ns_map, f)
        print('[ns_map] wrote %d host entries to %s' % (len(ns_map), NS_MAP_FILE))
    except Exception as e:
        print('[ns_map] WARNING: failed to write %s: %s' % (NS_MAP_FILE, e))

    # ── Summary (with zone breakdown) ────────────────────────────────────────
    zone_hosts = {}
    for (zone, _bldg, _edge) in topo.host_zone.values():
        zone_hosts[zone] = zone_hosts.get(zone, 0) + 1

    print('')
    print('=' * 60)
    print('  University Campus MTD Topology — %s' % size)
    print('=' * 60)
    print('  Core switches        : %d' % topo.n_core)
    print('  Building (agg) switch: %d' % topo.n_agg)
    print('  Access (edge) switch : %d' % topo.n_edge)
    print('  Total switches       : %d' % n_sw)
    print('  Total hosts          : %d  (10.0.0.1 .. )' % topo.n_hosts)
    print('  Switch<->switch links: %d undirected / %d directed' % (n_undirected, n_directed))
    print('  -' * 28)
    print('  Hosts per zone:')
    for zone in sorted(zone_hosts):
        print('    %-9s : %d hosts' % (zone, zone_hosts[zone]))
    print('  Buildings:')
    for (building, zone, access_hosts) in topo.plan:
        print('    %-16s [%-7s] %d access sw, %d hosts'
              % (building, zone, len(access_hosts), sum(access_hosts)))
    print('  Subnet               : 10.0.0.0/8 (flat; VIPs at 10.0.3.1+)')
    print('=' * 60)
    print('')

    CLI(net)
    net.stop()

    # Clean up per-run state files so a later run can't load stale data.
    for f in (NS_MAP_FILE, EXPECT_FILE):
        try:
            os.remove(f)
        except OSError:
            pass


if __name__ == '__main__':
    setLogLevel('info')
    size = sys.argv[1] if len(sys.argv) > 1 else 'medium'
    if size not in CAMPUS_PLANS:
        print('Usage: sudo python campus_topo.py <small|medium|large>')
        print('  got: %r' % size)
        sys.exit(1)
    run(size)
