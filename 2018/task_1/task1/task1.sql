SELECT
  id,
  max(date)  AS date,
  max(value) AS VALUE
FROM
  (SELECT task1.*
   FROM task1
     INNER JOIN
     (SELECT
        id,
        MAX(date) AS maxdate
      FROM task1
      GROUP BY id) t2
       ON task1.ID = t2.ID
          AND task1.date = t2.maxdate) AS t_maxdate
GROUP BY id
;