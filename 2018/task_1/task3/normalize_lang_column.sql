CREATE
TABLE langs (
  id INTEGER PRIMARY KEY,
  name TEXT UNIQUE
);


ALTER
TABLE tweets ADD COLUMN lang_id INTEGER DEFAULT NULL
;


SELECT DISTINCT
  lang as name
FROM tweets
WHERE lang IS NOT NULL
;


INSERT INTO langs (name)
    SELECT DISTINCT
  lower(lang) as name
FROM tweets
WHERE lang IS NOT NULL
;


UPDATE tweets
SET lang_id = (
  SELECT id
  FROM langs
  WHERE tweets.lang = langs.name
);