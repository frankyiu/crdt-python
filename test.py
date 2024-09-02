from concurrent.futures import ThreadPoolExecutor
from utils import BroadCaster
from service import RGAService


def test_rga():
    local_broadcaster = BroadCaster()
    a = RGAService(0, 3, local_broadcaster)
    b = RGAService(1, 3, local_broadcaster)
    c = RGAService(2, 3, local_broadcaster)
    local_broadcaster.register_listener(a)
    local_broadcaster.register_listener(b)
    local_broadcaster.register_listener(c)

    a.insert(0, 'a')
    b.insert(0, 'b')
    c.insert(0, 'c')

    assert a.to_view() == 'cba'
    assert b.to_view() == 'cba'
    assert c.to_view() == 'cba'

test_rga()

