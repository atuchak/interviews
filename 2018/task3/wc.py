import string
from collections import defaultdict

import sys


def wc(filename):
    words_counts = defaultdict(int)

    with open(filename) as f:
        for w in f.read().strip(string.whitespace).split():
            cleaned_word = w.strip(string.punctuation).lower()
            if cleaned_word:
                words_counts[cleaned_word] += 1

    return words_counts


def sort_wc(wc):
    wc_list = tuple(wc.items())

    def sort_func(obj):
        return -obj[1], obj[0]

    return sorted(wc_list, key=sort_func)


if __name__ == '__main__':
    words_counts = wc(sys.argv[1])
    sorted_wc = sort_wc(words_counts)
    for word, count in sorted_wc:
        print('{}: {}'.format(word, count))
