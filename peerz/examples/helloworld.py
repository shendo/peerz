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
import gevent

from peerz.core import Network
from peerz.transport import ConnectionError

SEED = 'localhost:7111:+v5-KOQCyCiShO3H98!$rz&lUnh=As:lal-)}Gc-'
SECRET = 'WP@84Uig:3U@qa^P[Q<MGoJj07ABmwgZRzOd}FfA'
ROOT = '/tmp/helloworld'

def main():
    port = 7111
    node = None
    while not node:
        try:
            net = Network(port, ROOT)
            # pregenerated keys for example seed
            if port == 7111:
                net.node.node_id = SEED.split(':', 2)[2]
                net.node.secret_key = SECRET
            net.join([SEED])
            node = net.get_local()
        except ConnectionError:
            port += 1
            print "Unable to bind to socket, trying next port: {0}".format(port)

    try:
        while True:
            print node
            if not net.get_peers():
                print " - No peers"

            for x in net.get_peers():
                print " - {0}".format(x.to_json())
            gevent.sleep(10)
    except KeyboardInterrupt:
        print 'Exiting...'
        net.leave()

if __name__ == '__main__':
    main()
