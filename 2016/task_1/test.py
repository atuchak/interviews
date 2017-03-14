from multiprocessing import Queue, Value, Lock

import multiprocessing

import pytest
import time

from .create_zips import generate_uniq_id
from .create_zips import worker as create_worker
from .process_zips import parse_xml, writer1, writer2, worker


def test_generate_uniq_id():
    assert isinstance(generate_uniq_id(1), list)
    assert isinstance(generate_uniq_id(2)[0], str)
    assert len(set(generate_uniq_id(10000))) == 10000


def test_create_worker():
    zip_queue = Queue()
    uniq_id_queue = Queue()

    p = multiprocessing.Process(
        target=create_worker,
        args=(zip_queue, uniq_id_queue,)
    )

    p.start()
    assert p.exitcode != 0
    time.sleep(1)
    assert p.exitcode == 0


def test_parse_xml():
    content = '''<root>
    <var name="id" value="uid-uuid-uuuid"/>
    <var name="level" value="44"/>
    <objects>
    <object name="qeqweqweqw"/>
    <object name="vdvdfsvdff" />
    </objects>
    </root>
    '''
    res = parse_xml(content)

    print(res)

    assert res['id'] == 'uid-uuid-uuuid'
    assert res['level'] == '44'

    assert len(res['object_names']) == 2
    assert res['object_names'][0] == 'qeqweqweqw'
    assert res['object_names'][1] == 'vdvdfsvdff'


@pytest.mark.parametrize('func', [writer1, writer2])
def test_writers(func):
    t_queue = Queue()
    num_of_active_workers = Value('i', 0)
    t_queue.put_nowait(dict(id='12', level='90', object_names=[]))

    p = multiprocessing.Process(target=func, args=(t_queue, num_of_active_workers,))
    p.start()
    p.join()
    assert p.exitcode == 0

    num_of_active_workers = Value('i', 1)
    p = multiprocessing.Process(target=func, args=(t_queue, num_of_active_workers,))
    p.start()
    num_of_active_workers.value = 0
    p.join()
    assert p.exitcode == 0


def test_worker():
    zips_queue = Queue()
    writer1_queue = Queue()
    writer2_queue = Queue()
    num_of_active_workers = Value('i', 1)
    lock = Lock()

    p = multiprocessing.Process(
        target=worker,
        args=(zips_queue, writer1_queue, writer2_queue, num_of_active_workers, lock,)
    )

    p.start()
    assert p.exitcode != 0
    time.sleep(1)
    assert p.exitcode == 0
    assert num_of_active_workers.value == 0

