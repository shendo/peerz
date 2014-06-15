peerz
=======

P2P python library using ZeroMQ sockets and gevent

Overview
--------

``peerz`` is an experiment in using zeromq bindings to implement a structured 
peer-to-peer overlay network in python.  The peer management and routing logic 
are heavily base on existing structured overlay networks such as
``kademlia``, ``tapestry`` and ``chord``. 

Goals
-----

The goal of ``peerz`` is to provide a p2p library in python capable of:

* Efficient routing in networks ranging from small LAN deployments to Internet scale
* Locality awareness of resources
* Scalable, fault tolerant and self-organising
* Abstraction of node discovery and management
* Simple generic API for which richer applications can be built on top of

Getting Started
---------------
Install using ``pip``: ::

	pip install peerz

Usage
-----

TODO

Issues
------

This project is still very much in its infancy, however, feedback is always welcome.
 
Source code for ``peerz`` is hosted on `GitHub`_. Any bug reports or feature
requests can be made using GitHub's `issues system`_.

.. _GitHub: https://github.com/shendo/peerz
.. _issues system: https://github.com/shendo/peerz/issues
