# coding: utf-8

import os
import ipaddress
import datetime
import math
import asyncio
import logging

from typing import (
    Coroutine,
    MutableSequence,
    NamedTuple,
    Optional,
    Tuple
)
from collections import deque

_logger = logging.getLogger(__name__)

class Ident:
    IDENT_BYTES = 20

    def __init__(self):
        self._bytes = os.urandom(self.IDENT_BYTES)

    def distance(self, other: 'Ident') -> int:
        buf = bytearray(self.IDENT_BYTES)
        for (i, (s, o)) in enumerate(zip(self._bytes, other._bytes)):
            buf[i] = s ^ o
        return int.from_bytes(buf, byteorder='little')

    def distance_log(self, other: 'Ident') -> int:
        dist = self.distance(other)
        # Could be much faster if we did msb. At that point we would need
        # to block it up by ints. However, we're not CPU bound so we just
        # do it the slow and easy way.
        return int(math.floor(math.log2(dist)))

    def int(self) -> int:
        return int.from_bytes(self._bytes, byteorder='little')

    def __hash__(self):
        return hash(self._bytes)

    def __str__(self):
        return f'Ident({self._bytes.hex()})'

    def __repr__(self):
        return str(self)

    def __eq__(self, other) -> bool:
        return (isinstance(other, Ident) and
                self._bytes == other._bytes)

class NodeDescriptor(NamedTuple):
    address: ipaddress.IPv4Address
    port: int
    ident: Ident

class NodeList:
    K = 20

    # TODO(amidvidy): figure out the type sig of last_look_cb.
    # Something weird like Callable[Awaitable?]
    def __init__(self, last_look_cb) -> None:
        # ordered by most recently seen.
        self._nodes: deque = deque([], self.K)
        self._last_look_cb = last_look_cb

    def __len__(self) -> int:
        return len(self._nodes)

    def freshest(self) -> Tuple[datetime.datetime, NodeDescriptor]:
        return self._nodes[0]

    def stalest(self) -> Tuple[datetime.datetime, NodeDescriptor]:
        return self._nodes[-1]

    def __contains__(self, node: NodeDescriptor) -> bool:
        f = list(filter(lambda t: t[1] == node, self._nodes))
        assert len(f) == 0 or len(f) == 1
        return len(f) == 1

    async def touch(self, node: NodeDescriptor):
        '''
        When a Kademlia node receives any message (request
        or reply) from another node, it updates the
        appropriate k-bucket for the sender’s node ID. If
        the sending node already exists in the recipient’s kbucket,
        the recipient moves it to the tail of the list.
        If the node is not already in the appropriate k-bucket
        and the bucket has fewer than k entries, then the recipient
        just inserts the new sender at the tail of the
        list. If the appropriate k-bucket is full, however, then
        the recipient pings the k-bucket’s least-recently seen
        node to decide what to do. If the least-recently seen
        node fails to respond, it is evicted from the k-bucket
        and the new sender inserted at the tail. Otherwise,
        if the least-recently seen node responds, it is moved
        to the tail of the list, and the new sender’s contact is
        discarded.
        '''
        now = datetime.datetime.utcnow()

        node_index = None
        # avoid modifying collection while iterating it.
        for i, (_, n) in enumerate(self._nodes):
            if n.ident == node.ident:
                assert (n.address, n.port) == (node.address, node.port)
                node_index = i
                break

        if node_index is not None:
            self._seen(now, node_index)
        elif len(self._nodes) < self.K:
            # We have room for this node. Add it to the front
            self._nodes.appendleft((now, node))
        else:
            # At capacity.
            (_, most_stale) = self._nodes[-1]
            to_keep = await self._last_look_cb(most_stale)
            if to_keep:
                self._seen(now, -1)
            else:
                self._nodes.appendleft((now, node))

    def _seen(self, at: datetime.datetime, node_index: int):
        (_, existing) = self._nodes[node_index]
        del self._nodes[node_index]
        self._nodes.appendleft((at, existing))

class NodeLists:
    pass

class LiveTransport:
    pass

# class SimulatedTransport:
#     def __init__(sender, receiver):
#         pass
#     async def ping(self, ):

class NodeHandle:
    def ident(self) -> Ident:
        raise NotImplementedError
    async def send_ping(self, target: 'NodeHandle'):
        raise NotImplementedError
    async def send_store(self):
        raise NotImplementedError
    async def send_find_node(self):
        raise NotImplementedError
    async def send_find_value(self):
        raise NotImplementedError

class RemoteNode(NodeHandle):
    def __init__(self, transport):
        self.transport = transport

class Node:
    def __init__(self, ident, driver):
        _logger.info(f'Creating node with ident {ident}')
        self._ident = Ident()
        self._driver = driver
        self._driver.bind(self)

        # need to keep track of inflight operations somehow.
        self._in_flight = {}

    def ident(self):
        return self._ident

    async def ping(self, target: NodeHandle) -> bool:
        _logger.info(f'Sending ping to {self.ident()}')
        rpc_ident = Ident()
        rpc_key = (target.ident(), rpc_ident)
        self._in_flight[rpc_key] = 'ping'
        await self._driver.send_ping(rpc_ident, target)

    # Incoming RPC handlers called by our driver.
    async def on_ping(self, rpc_ident: Ident(), sender: NodeHandle):
        rpc_key = (sender.ident(), rpc_ident)
        # If we previously sent an outbound ping, log the pong and exit.
        if rpc_key in self._in_flight and self._in_flight[rpc_key] == 'ping':
            del self._in_flight[rpc_key]
            _logger.info(f'{self.ident()} - Got pong from {sender.ident()}')
        else:
            # Received an inbound ping - send a pong.
            _logger.info(f'{self.ident()} - Got ping from {sender.ident()}, sending pong!')
            await self._driver.send_ping(rpc_ident, sender)

    async def on_store(self, sender: NodeHandle):
        pass
    async def on_find_node(self, sender: NodeHandle):
        pass
    async def on_find_value(self, sender: NodeHandle):
        pass

class LiveNodeDriver(asyncio.DatagramProtocol):
    def __init__(self):
        pass
    def connection_made(self, transport):
        pass
    def connection_lost(self, exc):
        pass
    def datagram_received(self, data, addr):
        pass
    def error_received(self, error_received, exc):
        pass

class SimulatedNodeDriver(NodeHandle):
    def __init__(self, network: 'SimulatedNetwork'):
        self._net = network
        self._node = None

    def ident(self):
        return self._node.ident()

    def bind(self, node: Node):
        self._node = node
        self._net.add(node.ident(), self)

    async def send_ping(self, rpc_ident, target):
        # Our node is the receiver in this case.
        receiver_handle = self._net.remote_handle(target.ident())
        await asyncio.sleep(0.1, loop=self._net.loop())
        await receiver_handle.receive_ping(rpc_ident, self)

    async def receive_ping(self, rpc_ident, inbound_handle):
        await self._node.on_ping(rpc_ident, inbound_handle)

class SimulatedNetwork:
    def __init__(self, loop):
        self._loop = loop
        self._nodes = {}

    def loop(self):
        return self._loop

    def add(self, ident: Ident, node_driver: SimulatedNodeDriver):
        _logger.info(f'Adding node {ident} to SimulatedNetwork.')
        self._nodes[ident] = node_driver

    def remote_handle(self, ident: Ident):
        assert ident in self._nodes
        return self._nodes[ident]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    net = SimulatedNetwork(loop)
    n1 = Node(Ident(), driver=SimulatedNodeDriver(net))
    n2 = Node(Ident(), driver=SimulatedNodeDriver(net))
    n3 = Node(Ident(), driver=SimulatedNodeDriver(net))

    async def p1to2():
        await n1.ping(n2)
    async def p2to3():
        await n2.ping(n3)
    async def p3to1():
        await n3.ping(n1)

    loop.run_until_complete(asyncio.wait([
        p1to2(),
        p2to3(),
        p3to1()
    ]))
    loop.close()
