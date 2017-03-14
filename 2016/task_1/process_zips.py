import csv
import os
import logging

import multiprocessing
import queue


from multiprocessing import Lock, Value

from lxml import etree
from multiprocessing import Queue
from multiprocessing import Process
from zipfile import ZipFile

OUTPUT_DIR = '/tmp/ngenix'

CONCURRENCY = multiprocessing.cpu_count()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def writer1(writer1_queue, num_of_active_workers):
    with open(os.path.join(OUTPUT_DIR, '1.csv'), 'w') as f:
        csv_writer = csv.writer(f)
        while True:
            try:
                data = writer1_queue.get_nowait()
                row = [data['id'], data['level']]
                csv_writer.writerow(row)

            except queue.Empty:
                if num_of_active_workers.value == 0:
                    break

    log.info('Writer1 is tearing down')


def writer2(writer2_queue, num_of_active_workers):
    with open(os.path.join(OUTPUT_DIR, '2.csv'), 'w') as f:
        csv_writer = csv.writer(f)
        while True:
            try:
                data = writer2_queue.get_nowait()
                for name in data['object_names']:
                    csv_writer.writerow([data['id'], name])

            except queue.Empty:
                if num_of_active_workers.value == 0:
                    break

    log.info('Writer2 is tearing down')


def parse_xml(content):
    root = etree.fromstring(content)
    try:
        element = root.xpath("//var[@name='id']")[0]
        id = element.attrib['value']

        element = root.xpath("//var[@name='level']")[0]
        level = element.attrib['value']

        elements = root.xpath("//objects/*")
        object_names = []
        for o in elements:
            object_names.append(o.attrib['name'])

    except (IndexError, KeyError):
        log.error('var tag with value not found')
        return None

    return {'id': id, 'level': level, 'object_names': object_names}


def worker(zips_queue, writer1_queue, writer2_queue, num_of_active_workers, lock):
    while True:
        try:
            file = zips_queue.get_nowait()
            log.info('Processing {}'.format(file))
            with ZipFile(os.path.join(OUTPUT_DIR, file), 'r') as myzip:
                for xml_file in myzip.filelist:
                    content = myzip.read(xml_file)
                    res = parse_xml(content)

                    if res:
                        writer1_queue.put_nowait(res)
                        writer2_queue.put_nowait(res)

        except queue.Empty:
            with lock:
                num_of_active_workers.value -= 1

            break

    log.info('Worker is tearing down')


def main():
    writer1_queue = Queue()
    writer2_queue = Queue()
    zips_queue = Queue()
    lock = Lock()
    num_of_active_workers = Value('i', CONCURRENCY)

    files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.zip')]
    for f in files:
        zips_queue.put(f)

    writer1_process = Process(target=writer1, args=(writer1_queue, num_of_active_workers,))
    writer1_process.start()

    writer2_process = Process(target=writer2, args=(writer2_queue, num_of_active_workers,))
    writer2_process.start()

    workers_list = []

    for _ in range(CONCURRENCY):
        p = Process(target=worker, args=(zips_queue, writer1_queue, writer2_queue, num_of_active_workers, lock,))
        p.start()
        workers_list.append(p)


    writer1_process.join()
    writer2_process.join()

    for p in workers_list:
        p.join()

    writer1_queue.close()
    writer2_queue.close()
    zips_queue.close()


if __name__ == '__main__':
    main()
