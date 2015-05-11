import csv
import random


class DummyColorMap(object):
    def __getitem__(self, *args):
        return ''


def csv_rows(filename):
    '''
    Given a filename, opens a csv file and yeilds it line by line.
    '''
    with open(filename, 'r') as csvfile:
        for row in csv.reader(csvfile):
            yield row


def random_list(gen=None, n=None):
    if gen is None:
        def gen():
            return random.randint(-1000, 1000)
    if n is None:
        def length():
            return random.randint(1, 5)
    else:
        def length():
            return n

    return [gen() for _ in range(length())]
