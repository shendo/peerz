# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014 Steve Henderson
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

import os

try:
    import cPickle
    serialiser = cPickle
except:
    cPickle = None
    import pickle as serialiser 

class LocalStorage(object):
    
    def __init__(self, root, port):
        self.port = port
        self.rootpath = os.path.join(root, str(port))
        try:
            os.makedirs(self.rootpath)
        except OSError:
            pass
        
    def store(self, key, contents, node_id=None):        
        if node_id:
            path = os.path.join(self.rootpath, node_id)
        else:
            path = self.rootpath
        
        try:
            os.makedirs(path)
        except OSError:
            pass
        
        with open(os.path.join(path, key), 'wb') as tmp:
            tmp.write(serialiser.dumps(contents))

    def fetch(self, key, node_id=None):
        if node_id:
            path = os.path.join(self.rootpath, node_id, key)
        else:
            path = os.path.join(self.rootpath, key)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as tmp:
            return serialiser.loads(tmp.read())

        