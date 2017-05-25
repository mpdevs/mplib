USE das;
ADD FILE /home/udflib/udf_edit_distance3.py;
DROP TABLE IF EXISTS ci_diaper2014to2016_0406_allplatform_distinct_edit_distance;
CREATE TABLE ci_diaper2014to2016_0406_allplatform_distinct_edit_distance (
    id1 STRING,
    id2 STRING,
    edit_distance INT
);
INSERT INTO ci_diaper2014to2016_0406_allplatform_distinct_edit_distance
SELECT TRANSFORM(
    content1,
    content2,
    id1,
    id2
)
USING 'python udf_edit_distance3.py' AS (
    id1,
    id2,
    edit_distance
)
FROM (
    SELECT id AS id1, content AS content1
    FROM ci_diaper2014to2016_0406_allplatform_distinct AS d1
    WHERE d1.year = '2016'
) AS a
CROSS JOIN (
    SELECT id AS id2, content AS content2
    FROM ci_diaper2014to2016_0406_allplatform_distinct AS d2
    WHERE d2.year = '2016'
) AS b;

