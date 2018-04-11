import os
import glob
import multiprocessing
import csv
from collections import defaultdict
from itertools import chain


class MyMapReduce(object):
    def __init__(self, map_func, reduce_func, num_workers=1):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    @staticmethod
    def partition(mapped_values):
        partitioned_data = defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs):
        map_responses = self.pool.map(self.map_func, inputs)
        partitioned_data = self.partition(chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values


def prefilter_file(filename):
    output = []
    with open(filename, 'r') as f:
        csvreader = csv.reader(f, delimiter=',')
        next(csvreader)  # skip header
        for row in csvreader:
            if len(row) != 4:
                continue
            output.append((filename, row,))
    return output


def write_prefiltered_file(item):
    filename, rows = item
    new_filename = filename.replace('/incoming/', '/staging/')
    os.makedirs(os.path.dirname(new_filename), exist_ok=True)
    with open(new_filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    return new_filename


def map_archive(filename):
    output = []
    with open(filename, 'r') as f:
        csvreader = csv.reader(f, delimiter=',')
        for row in csvreader:
            business_date = row[-1]
            output.append((business_date, (filename, row,),))
    return output


def write_archived(item):
    business_date, data = item
    rows_grouped_by_filename = defaultdict(list)
    for filename, row in data:
        rows_grouped_by_filename[filename].append(row)

    for filename in rows_grouped_by_filename.keys():
        new_filename = filename.replace('/staging/', '/archive/' + business_date.strip() + '/')
        os.makedirs(os.path.dirname(new_filename), exist_ok=True)
        with open(new_filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(rows_grouped_by_filename[filename])


if __name__ == '__main__':
    max_workers = 2
    incoming_files = glob.glob('data/incoming/*.csv')
    print(f'incoming files = {incoming_files}')

    prefilter_mapper = MyMapReduce(prefilter_file, write_prefiltered_file, num_workers=max_workers)
    prefiltered_files = prefilter_mapper(incoming_files)
    print(f'prefiltered_files = {prefiltered_files}')

    archive_mapper = MyMapReduce(map_archive, write_archived, num_workers=max_workers)
    archive = archive_mapper(prefiltered_files)
    archived_files = glob.glob('data/archive/*/*')
    print(f'archived_files = {archived_files}')
