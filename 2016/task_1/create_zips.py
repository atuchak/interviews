import multiprocessing
import os
import queue
import random
import shutil
import string
import uuid
from zipfile import ZipFile

from lxml import etree
from multiprocessing import Process
from multiprocessing import Queue


NUM_OF_ZIPS = 50
NUM_OF_FILES_IN_ZIP = 100
OUTPUT_DIR = '/tmp/ngenix'

try:
    shutil.rmtree(OUTPUT_DIR)
except FileNotFoundError:
    pass
finally:
    os.mkdir(OUTPUT_DIR)

CONCURRENCY = multiprocessing.cpu_count()


def generate_uniq_id(max_count):
    uids = [str(uuid.uuid4()) for _ in range(max_count)]
    if len(uids) != max_count:
        uids_set = set(uids)
        while len(uids_set) != max_count:
            uids_set.add(str(uuid.uuid4()))
        return list(uids_set)

    return uids


def create_xml(filename, uniq_id_queue):
    root = etree.Element('root')

    child = etree.Element('var')
    child.set('name', 'id')
    child.set('value', uniq_id_queue.get())
    root.append(child)

    child = etree.Element('var')
    child.set('name', 'level')
    child.set('value', str(random.randrange(1,100)))
    root.append(child)

    child = etree.Element('objects')

    for _ in range(random.randrange(10)):
        sub = etree.Element('object')
        rand_name = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
        sub.set('name', rand_name)
        child.append(sub)

    root.append(child)

    return dict(filename=filename, content=etree.tostring(root, pretty_print=True))


def create_zip(filename, xmls):
    with ZipFile(filename, 'w') as myzip:
        for e in xmls:
            myzip.writestr(e['filename'], e['content'])


def worker(zip_queue, uniq_id_queue):
    while True:
        try:
            filename = zip_queue.get(timeout=0.5)

            xmls_list = []
            for f in range(NUM_OF_FILES_IN_ZIP):
                xml_filename = str(f) + '.xml'
                xmls_list.append(create_xml(xml_filename, uniq_id_queue))

            create_zip(os.path.join(OUTPUT_DIR, filename), xmls_list)
        except queue.Empty:
            print('all done, worker is tearing down')
            break


def main():
    zip_queue = Queue()
    uniq_id_queue = Queue()
    filenames = [str(x) + '.zip' for x in range(NUM_OF_ZIPS)]

    for f in filenames:
        zip_queue.put(f)

    for uid in generate_uniq_id(NUM_OF_ZIPS*NUM_OF_FILES_IN_ZIP):
        uniq_id_queue.put(uid)

    process_list = []
    for _ in range(CONCURRENCY):
        p = Process(target=worker, args=(zip_queue, uniq_id_queue,))
        process_list.append(p)
        p.start()

    for p in process_list:
        p.join()

    zip_queue.close()


if __name__ == '__main__':
    main()
