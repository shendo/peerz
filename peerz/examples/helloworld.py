# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014-2015 Steve Henderson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time

from peerz import Network

SEED = 'localhost:7111:7%OJ6BCEW:}0MBW10DZWWB0.a<za!7F*U.31qpYX'
NODE_ID = '7%OJ6BCEW:}0MBW10DZWWB0.a<za!7F*U.31qpYX'
SECRET = 'AL[w!D]I8jpmJ09ajSD7Vmw.$MA@Ld5VT!Sj$R!V'

def main():
    net = Network([SEED], storage='/tmp/helloworld')
    node = net.get_local()
    if node['port'] == 7111:
        node = net.join(NODE_ID, SECRET)
        net.publish('hello', 'world')
    else:
        net.join()
    try:
        while True:
            node = net.get_local()
            peers = net.get_peers()
            print "%s:%s:%s (%s)" % (node['address'],
                                     node['port'],
                                     node['node_id'],
                                     node['hostname']) 
            if not peers:
                print " - No peers"
            for x in peers:
                print " - %s:%s:%s (%s) %s" % (x.pop('address'),
                                               x.pop('port'),
                                               x.pop('node_id'),
                                               x.pop('hostname'),
                                               str(x))
            print ""
            time.sleep(10)
            print 'hello = ' + (net.fetch('hello') or 'unknown')
    except KeyboardInterrupt:
        print 'Exiting...'
        net.leave()

if __name__ == '__main__':
    main()
