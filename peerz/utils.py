# =========================================================================
# zactor - simple actor framework
#
# Copyright (c) the Contributors as noted in the AUTHORS file.
# This file is part of CZMQ, the high-level C binding for 0MQ:
# http://czmq.zeromq.org.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# =========================================================================
#
#
# The zactor class provides a simple actor framework. It replaces the
# zthread class, which had a complex API that did not fit the CLASS
# standard. A CZMQ actor is implemented as a thread plus a PAIR-PAIR
# pipe. The constructor and destructor are always synchronized, so the
# caller can be sure all resources are created, and destroyed, when these
# calls complete. (This solves a major problem with zthread, that a caller
# could not be sure when a child thread had finished.)
#
# A zactor_t instance acts like a zsock_t and you can pass it to any CZMQ
# method that would take a zsock_t argument, including methods in zframe,
# zmsg, zstr, zpoller, and zloop.

# The following is derived from https://github.com/zeromq/pyre
# which itself appears to be ported from http://czmq.zeromq.org

import random
import struct
import zmq
import threading
import logging

logger = logging.getLogger(__name__)

class Pipe(zmq.Socket):

    PIPE_SIGNAL = 0x7766554433221100

    def __init__(self, *args, **kwargs):
        super(zmq.Socket, self).__init__(*args, **kwargs)

    def signal(self, status=0):
        self.send(struct.pack("Q", Pipe.PIPE_SIGNAL + status))

    def wait(self):
        while(True):
            msg = self.recv()
            if len(msg) == 8:
                signal_value = struct.unpack('Q', msg)[0]
                if (signal_value & 0xFFFFFFFFFFFFFF00) == Pipe.PIPE_SIGNAL:
                    return signal_value & 255
                else:
                    return -1

# Create a pipe, which consists of two PAIR sockets connected over inproc.
# The pipe is configured to use a default 1000 hwm setting. Returns the
# frontend and backend sockets.
def create_pipe(ctx, hwm=1000):
    backend = Pipe(ctx, zmq.PAIR)
    frontend = Pipe(ctx, zmq.PAIR)
    backend.set_hwm(hwm)
    frontend.set_hwm(hwm)
    backend.setsockopt(zmq.LINGER, 0)
    frontend.setsockopt(zmq.LINGER, 0)
    endpoint = "inproc://pipe-%04x-%04x\n"\
                 % (random.randint(0, 0x10000), random.randint(0, 0x10000))
    while True:
        try:
            frontend.bind(endpoint)
        except:
            endpoint = "inproc://pipe-%04x-%04x\n"\
                 % (random.randint(0, 0x10000), random.randint(0, 0x10000))
        else:
            break
    backend.connect(endpoint)
    return (frontend, backend)

class Actor(object):

    ACTOR_TAG = 0x0005cafe

    def __init__(self, ctx, actor, *args, **kwargs):
        self.tag = self.ACTOR_TAG
        self.ctx = ctx
        # Create front-to-back pipe pair
        self.pipe, self.shim_pipe = create_pipe(ctx)
        self.shim_handler = actor
        self.shim_args = (self.ctx, self.shim_pipe) + args
        self.shim_kwargs = kwargs
        self.thread = threading.Thread(target=self.run)
        # we manage threads exiting ourselves!
        self.thread.daemon = False
        self.thread.start()

        # Mandatory handshake for new actor so that constructor returns only
        # when actor has also initialized. This eliminates timing issues at
        # application start up.
        self.pipe.wait()

    def run(self):
        self.shim_handler(*self.shim_args, **self.shim_kwargs)
        self.shim_pipe.set(zmq.SNDTIMEO, 0)
        self.shim_pipe.signal()
        self.shim_pipe.close()

    def destroy(self):
        # Signal the actor to end and wait for the thread exit code
        # If the pipe isn't connected any longer, assume child thread
        # has already quit due to other reasons and don't collect the
        # exit signal.
        if self.tag == 0xDeadBeef:
            logger.warning("Actor: already destroyed")
            return
        self.pipe.set(zmq.SNDTIMEO, 0)
        self.pipe.send_unicode("$TERM")
        # misschien self.pipe.wait()????
        self.pipe.wait()
        self.pipe.close()
        self.tag = 0xDeadBeef

    def send(self, *args, **kwargs):
        return self.pipe.send(*args, **kwargs)

    def send_unicode(self, *args, **kwargs):
        return self.pipe.send_unicode(*args, **kwargs)

    def send_multipart(self, *args, **kwargs):
        return self.pipe.send_multipart(*args, **kwargs)

    def send_pyobj(self, *args, **kwargs):
        return self.pipe.send_pyobj(*args, **kwargs)

    def recv(self, *args, **kwargs):
        return self.pipe.recv(*args, **kwargs)

    def recv_unicode(self, *args, **kwargs):
        return self.pipe.recv_unicode(*args, **kwargs)

    def recv_multipart(self, *args, **kwargs):
        return self.pipe.recv_multipart(*args, **kwargs)

    def recv_pyobj(self, *args, **kwargs):
        return self.pipe.recv_pyobj(*args, **kwargs)

    # --------------------------------------------------------------------------
    # Probe the supplied object, and report if it looks like a zactor_t.
    def is_actor(self):
        return isinstance(Actor, self)

    # --------------------------------------------------------------------------
    # Probe the supplied reference. If it looks like a zactor_t instance,
    # return the underlying libzmq actor handle; else if it looks like
    # a libzmq actor handle, return the supplied value.
    # In Python we just return the pipe socket
    def resolve(self):
        return self.pipe
