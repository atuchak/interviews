import sqlite3
import string
from pprint import pprint

from collections import defaultdict


def init_db():
    dbconn = sqlite3.connect('test.db')
    return dbconn


def load_afinn(filename):
    with open(filename, 'r') as f:
        data = ((e.split('\t')[0], int(e.split('\t')[1].strip())) for e in f)
        return dict(data)


def get_tweets(dbconn):
    cursor = dbconn.cursor()
    sql_query = 'SELECT id, tweet_text FROM tweets;'
    cursor.execute(sql_query)
    for row in cursor:
        yield dict(id=row[0], text=row[1])


def get_words_from_text(text):
    words = text.split(' ')
    return [w.strip(string.punctuation + string.whitespace) for w in words]


def calc_tweet_sentiment(tweet_text, sentiment_map):
    words = get_words_from_text(tweet_text)
    sentiment = 0
    sentiment_map = defaultdict(lambda: 0, sentiment_map)
    for word in words:
        sentiment += sentiment_map[word]
    return sentiment


def update_tweet_sentiment(dbconn, tweet_id, sentiment):
    update_query = 'UPDATE tweets SET tweet_sentiment = :sentiment WHERE id = :id;'
    cursor = dbconn.cursor()
    cursor.execute(update_query, {'id': tweet_id, 'sentiment': sentiment})
    dbconn.commit()


def main():
    dbconn = init_db()
    filename = 'AFINN-111.txt'
    sentiment_map = load_afinn(filename)
    tweets = get_tweets(dbconn)

    for tweet in tweets:
        sentiment = calc_tweet_sentiment(tweet['text'], sentiment_map)
        update_tweet_sentiment(dbconn, tweet['id'], sentiment)


if __name__ == '__main__':
    main()
