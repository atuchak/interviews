import json
import sqlite3

from pprint import pprint


def load_tw_from_file(file):
    with open(file, 'r') as f:
        for c, line in enumerate(f):
            data = json.loads(line)
            if 'created_at' in data:
                yield data

            if 'delete' in data:
                pass


def filter_tweet_data(tweet_data):
    assert isinstance(tweet_data, dict)
    if not isinstance(tweet_data['place'], dict):
        tweet_data['place'] = {}

    return dict(
        name=tweet_data['user']['name'],
        tweet_text=tweet_data['text'],
        country_code=tweet_data.get('place', {}).get('country_code'),
        display_url=tweet_data['user']['url'],
        lang=tweet_data['lang'].lower(),
        created_at=tweet_data['created_at'],
        location=tweet_data['user']['location'],
    )


def init_db():
    dbconn = sqlite3.connect('test.db')
    cursor = dbconn.cursor()
    create_table_sql = '''\
    CREATE TABLE IF NOT EXISTS tweets (
    id INTEGER PRIMARY KEY,
    name TEXT,
    tweet_text TEXT,
    country_code TEXT,
    display_url TEXT,
    lang TEXT,
    created_at TEXT,
    location TEXT
    );
    '''
    cursor.execute(create_table_sql)
    dbconn.commit()
    return dbconn


def insert_tweets_into_db(dbconn, tweets):
    batch_size = 100
    cursor = dbconn.cursor()
    sql = '''\
    INSERT INTO tweets
    (name, tweet_text, country_code, display_url, lang, created_at, location)
    VALUES(:name, :tweet_text, :country_code, :display_url, :lang, :created_at, :location);
    '''
    counter = 0
    for tw in tweets:
        cursor.execute(sql, tw)
        counter += 1
        if counter > batch_size:
            dbconn.commit()

    dbconn.commit()


def add_tweet_sentiment_column(dbconn):
    cursor = dbconn.cursor()
    sql = '''\
    ALTER TABLE tweets ADD COLUMN tweet_sentiment INTEGER DEFAULT 0;
    '''

    cursor.execute(sql)
    dbconn.commit()


def main():
    file = 'three_minutes_tweets.json.txt'
    tweets = load_tw_from_file(file)
    dbconn = init_db()

    cleaned_tweets = (filter_tweet_data(tweet) for tweet in tweets)
    insert_tweets_into_db(dbconn, cleaned_tweets)

    add_tweet_sentiment_column(dbconn)


if __name__ == '__main__':
    main()
