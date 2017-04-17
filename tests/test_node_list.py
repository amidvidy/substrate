import pytest
import ipaddress
import random
import datetime

from substrate.node import Ident, NodeList, NodeDescriptor

def gen_node() -> NodeDescriptor:
    return NodeDescriptor(
        address=ipaddress.IPv4Address(random.randint(0, 2**32)),
        port=random.randint(1, 2**16),
        ident=Ident(),
    )

@pytest.fixture
async def node_list_keep():
    async def return_true(_):
        return True
    return NodeList(return_true)

@pytest.fixture
async def node_list_drop():
    async def return_false(_):
        return False
    return NodeList(return_false)

@pytest.mark.asyncio
async def test_node_list_seen_empty(node_list_keep):
    nl = await node_list_keep
    n = gen_node()
    assert len(nl) == 0
    await nl.touch(n)
    assert len(nl) == 1

@pytest.mark.asyncio
async def test_node_list_touch_order(node_list_keep):
    nlst = await node_list_keep
    n1 = gen_node()
    n2 = gen_node()
    assert not n1 in nlst
    assert not n2 in nlst
    await nlst.touch(n1)
    await nlst.touch(n2)
    assert n1 in nlst
    assert n2 in nlst
    assert len(nlst) == 2
    assert nlst.freshest()[1].ident == n2.ident
    assert nlst.stalest()[1].ident == n1.ident
    await nlst.touch(n1)
    assert len(nlst) == 2
    assert nlst.freshest()[1].ident == n1.ident
    assert nlst.stalest()[1].ident == n2.ident

@pytest.mark.asyncio
async def test_node_capacity_keep(node_list_keep):
    nlst = await node_list_keep
    for _ in range(NodeList.K):
        await nlst.touch(gen_node())
    stalest = nlst.stalest()[1]
    newcomer = gen_node()
    assert stalest in nlst
    await nlst.touch(newcomer)
    assert stalest in nlst
    assert newcomer not in nlst
    assert nlst.freshest()[1] == stalest

@pytest.mark.asyncio
async def test_node_capacity_drop(node_list_drop):
    nlst = await node_list_drop
    for _ in range(NodeList.K):
        await nlst.touch(gen_node())
    stalest = nlst.stalest()[1]
    newcomer = gen_node()
    assert stalest in nlst
    await nlst.touch(newcomer)
    assert stalest not in nlst
    assert newcomer in nlst
    assert nlst.freshest()[1] == newcomer
