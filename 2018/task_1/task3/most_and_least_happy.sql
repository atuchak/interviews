-- country having max sentiment
SELECT country_code, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE country_code IS NOT NULL AND location <> ''
GROUP BY country_code
ORDER BY sum_sentiment DESC
LIMIT 1
;

-- country having min sentiment
SELECT country_code, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE country_code IS NOT NULL AND location <> ''
GROUP BY country_code
ORDER BY sum_sentiment ASC
LIMIT 1
;

-- user having max sentiment
SELECT name, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE name IS NOT NULL AND location <> ''
GROUP BY name
ORDER BY sum_sentiment DESC
LIMIT 1
;

-- user having min sentiment
SELECT name, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE name IS NOT NULL AND location <> ''
GROUP BY name
ORDER BY sum_sentiment ASC
LIMIT 1
;

-- location having max sentiment
SELECT location, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE location IS NOT NULL AND location <> ''
GROUP BY location
ORDER BY sum_sentiment DESC
LIMIT 1
;

-- location having min sentiment
SELECT location, sum(tweet_sentiment) sum_sentiment
FROM tweets
WHERE location IS NOT NULL AND location <> ''
GROUP BY location
ORDER BY sum_sentiment ASC
LIMIT 1
;