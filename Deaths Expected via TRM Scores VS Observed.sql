
# ND High
    SELECT
    a.TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '<13.1' AS TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
UNION SELECT
    a.TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '>=13.1' AS TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
UNION SELECT
    a.TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '<13.1' AS TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
UNION SELECT
    a.TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '>=13.1' AS TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)

;

SELECT * FROM protocollist.intensitymapping;

# GCLAM ND
    SELECT
    a.TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '<13.1' AS TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
UNION SELECT
    a.TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '>=13.1' AS TRMRange,
    'GCLAM ND Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND StatusDisease LIKE '%ND%'
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
UNION

# R/R GCLAM
SELECT
    a.TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '<13.1' AS TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (1 , 2, 3)
UNION SELECT
    a.TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)
GROUP BY a.TRMRangeOrder
UNION SELECT
    '>=13.1' AS TRMRange,
    'GCLAM R/R Patients' AS Population,
    'High' AS `Treatment Intensity`,
    COUNT(*) AS `Patient Arrivals`,
    SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
    CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                AS CHARACTER (5)),
            '/',
            COUNT(*),
            ' (',
            ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                    1),
            '%)') AS `Death/Arrivals`,
    ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
    ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
FROM
    Temp.TRMRangeOrder a
        LEFT JOIN
    (SELECT
        *
    FROM
        Temp.ArrivalTRM_GCLAM
    WHERE
        Categorized <> 'NOT TREATED'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
WHERE
    a.TRMRangeOrder IN (4 , 5, 6, 7)

UNION

# ND High
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
    UNION

# ND intermediate
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
    UNION
# ND low
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'ND Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
union

# R/R High
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)

union

# R/R Intermediate
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'Intermediate'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)

union

# R/R Low
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '<13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (1,2,3)
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(
            cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
            ,'/'
            ,count(*)
            , ' ('
            , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
            , '%)'
            ) AS `Death/Arrivals`

        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)
        GROUP by a.TRMRangeOrder
    UNION
    SELECT '>=13.1' AS TRMRange
        , 'R/R Patients' AS Population
        , 'Low'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , concat(SUM(IF(ResponseDescription='Death',1,0)),'/',count(*)) AS `Death/Arrivals`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        WHERE a.TRMRangeOrder in (4,5,6,7)

;


