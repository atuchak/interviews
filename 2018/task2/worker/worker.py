import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from functools import wraps
from json import JSONDecodeError
from queue import Empty
from threading import Thread
from time import sleep

import pika
import psycopg2
import wget
from multiprocessing import Process

from multiprocessing import Queue

MAX_PROCESS = 4
DB_SETTINGS = 'dbname=testdb user=postgres host=postgres'
RABBITMQ_HOST = 'rabbitmq'

DB_SETTINGS = 'dbname=testdb user=postgres host=localhost'
RABBITMQ_HOST = 'localhost'


@contextmanager
def get_url_as_file(url):
    file = wget.download(url, bar=None)
    with open(file, 'rb') as fd:
        yield fd
    os.remove(file)


def calc_md5(f):
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: f.read(4096), b''):
        hash_md5.update(chunk)
    return hash_md5.hexdigest()


def process_md5(url):
    with get_url_as_file(url) as f:
        sleep(1)  # it's a heavy task
        return calc_md5(f)


def worker(results_queue, url, task_id):
    print('starting worker process')
    has_error = False
    try:
        result = process_md5(url)
    except Exception as e:
        result = str(e)
        has_error = True
    finally:
        results_queue.put_nowait({'result': result, 'has_error': has_error, 'task_id': task_id})


def spawn_workers(workers, tasks_queue, results_queue):
    while True:
        if len(workers) < MAX_PROCESS:
            try:
                task = tasks_queue.get(timeout=1)
                url = task['url']
                task_id = task['task_id']
                worker_process = Process(target=worker, args=(results_queue, url, task_id))
                worker_process.start()
                workers.update({worker_process.pid: worker_process})
            except Empty:
                pass


def cleanup_worker_process(workers):
    dead_workers_pids = []
    while True:
        for pid, wp in workers.items():
            if wp is not None and not wp.is_alive():
                print(f'removing dead process {pid}')
                wp.join()
                dead_workers_pids.append(pid)

        for _ in range(len(dead_workers_pids)):
            del workers[dead_workers_pids.pop()]

        sleep(0.1)


def consume_results(f):
    @wraps(f)
    def inner(results_queue):
        while True:
            try:
                result = results_queue.get(timeout=1)
                f(result)
            except Empty:
                pass

    return inner


@consume_results
def put_results_to_db(data):
    dbconn = psycopg2.connect(DB_SETTINGS)
    cursor = dbconn.cursor()

    result = data['result']
    task_id = str(data['task_id'])
    has_error = data['has_error']

    if has_error:
        sql_query = 'UPDATE api_md5task SET result = %s, has_error = TRUE WHERE guid = %s;'
    else:
        sql_query = 'UPDATE api_md5task SET result = %s, has_error = FALSE WHERE guid = %s;'

    cursor.execute(sql_query, (result, task_id,))
    dbconn.commit()
    print(f'task is done {task_id}')
    cursor.close()
    dbconn.close()


def init_rabbitmq(tasks_queue):
    def md5task_callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            data['task_id'] = uuid.UUID(data['task_id'])
            tasks_queue.put_nowait(data)
        except JSONDecodeError:
            pass
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    while True:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue='md5tasks')
        channel.basic_consume(md5task_callback, queue='md5tasks', no_ack=False)
        channel.start_consuming()
        sleep(0.1)


def main():
    tasks_queue = Queue()
    results_queue = Queue()

    rabbit_thread = Thread(target=init_rabbitmq, args=(tasks_queue,))
    rabbit_thread.start()

    workers = {}

    worker_spawner_thread = Thread(target=spawn_workers, args=(workers, tasks_queue, results_queue,))
    worker_spawner_thread.start()

    cleanup_worker_process_thread = Thread(target=cleanup_worker_process, args=(workers,))
    cleanup_worker_process_thread.start()

    db_thread = Thread(target=put_results_to_db, args=(results_queue,))
    db_thread.start()

    while True:
        try:
            print(f'num of workers = {len(workers)}')
            sleep(2)
        except KeyboardInterrupt:
            break

    rabbit_thread.join()
    worker_spawner_thread.join()
    for w in workers.values():
        w.join()
    cleanup_worker_process_thread.join()
    cleanup_worker_process_thread.join()
    db_thread.join()


if __name__ == '__main__':
    main()
