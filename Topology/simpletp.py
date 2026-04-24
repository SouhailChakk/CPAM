#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel
import sys


class MTDTopo(Topo):
    """
    Simple two-switch MTD topology with configurable host count.

    Hosts are assigned flat sequential IPs on 10.0.0.x/24:
      h1  -> 10.0.0.1
      h2  -> 10.0.0.2
      ...
      hN  -> 10.0.0.N

    Real host range  : 10.0.0.1  - 10.0.0.128  (max 128 hosts)
    VIP range        : 10.0.0.129 - 10.0.0.253  then 10.0.1.x onward
    Controller probe : 10.0.0.254 (reserved, never a host)

    Hosts are distributed evenly between s1 and s2.
    s1 and s2 are connected to each other.
    """

    def __init__(self, num_hosts=4, **opts):
        self.num_hosts = num_hosts
        super(MTDTopo, self).__init__(**opts)

    def build(self):
        if self.num_hosts < 1 or self.num_hosts > 128:
            raise ValueError("num_hosts must be between 1 and 128")

        # Two switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')

        # Add hosts with flat sequential 10.0.0.x IPs.
        # defaultRoute ensures the kernel forwards packets destined for
        # VIPs on 10.0.1.x (or any non-local subnet) to the switch
        # instead of dropping them with "Network is unreachable".
        # 10.0.0.254 is the controller's ARP probe IP — it acts as the
        # gateway address. The controller intercepts traffic anyway so
        # the gateway never needs to be a real host.
        hosts = []
        for i in range(1, self.num_hosts + 1):
            ip   = '10.0.0.%d/24' % i
            mac  = '00:00:00:00:00:%02x' % i
            host = self.addHost(
                'h%d' % i,
                ip=ip,
                mac=mac,
                defaultRoute='via 10.0.0.254'
            )
            hosts.append(host)

        # Distribute hosts evenly between the two switches
        for i, host in enumerate(hosts):
            if i % 2 == 0:
                self.addLink(host, s1)
            else:
                self.addLink(host, s2)

        # Connect the two switches
        self.addLink(s1, s2)


def run(num_hosts=4):
    topo = MTDTopo(num_hosts=num_hosts)
    net  = Mininet(
        topo=topo,
        controller=RemoteController,
        switch=OVSKernelSwitch
    )
    net.start()
    print('[INFO] Topology: %d hosts on 10.0.0.1-10.0.0.%d' % (num_hosts, num_hosts))
    print('[INFO] VIPs start at 10.0.0.129')
    print('[INFO] Controller probe IP: 10.0.0.254 (reserved)')
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')

    num_hosts = 4   # default: original 4-host setup
    if len(sys.argv) > 1:
        try:
            num_hosts = int(sys.argv[1])
            if num_hosts < 1 or num_hosts > 128:
                print('[ERROR] num_hosts must be between 1 and 128')
                sys.exit(1)
            print('[INFO] Creating topology with %d hosts' % num_hosts)
        except ValueError:
            print('[ERROR] Invalid number of hosts. Using default: 4')

    run(num_hosts)
