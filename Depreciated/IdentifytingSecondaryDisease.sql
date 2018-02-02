DROP TABLE IF EXISTS temp1;
CREATE TABLE temp1 AS
SELECT
    a.PtMRN,
    a.PatientId,
    a.PtFirstName,
    a.PtLastName,
    b.ComorbDate,
    b.Comorbidity,
    b.ComorbNotes,
    c.StatusDisease,
    CASE
        WHEN b.Comorbidity = 'AHD' THEN 'AHD'
        WHEN b.Comorbidity = 'Chemotherapy History' THEN 'Chemotherapy History'
        WHEN c.StatusDisease LIKE '%nd2%' THEN 'Secondary AML (reason unknown)'
        WHEN c.StatusDisease LIKE '%nd1%' THEN 'De Novo AML'
        ELSE 'Uncertain, probably De Novo'
    END AS SecondaryType,
    CASE
        WHEN b.Comorbidity = 'AHD'
            OR b.Comorbidity = 'Chemotherapy History'
            OR c.StatusDisease LIKE '%nd2%'
        THEN '1 Secondary'
        WHEN c.StatusDisease LIKE '%nd1%'
        THEN '2 De Novo'
        ELSE '3 Probably De Novo'
    END AS Secondary
FROM
    caisis.vdatasetpatients a
        LEFT JOIN
    (SELECT * FROM caisis.vdatasetcomorbidities WHERE Comorbidity IN ('AHD','Chemotherapy History') ) b ON a.PtMRN = b.PtMRN
        LEFT JOIN
    (SELECT * FROM caisis.vdatasetstatus WHERE StatusDisease LIKE '%nd%') c on a.PtMRN = c.PtMRN
WHERE
    a.PtMRN IN ('U0277965' ,
        'U1188252',
        'U2170361',
        'U2548804',
        'U2560923',
        'U2635054',
        'U2645307',
        'U3008782',
        'U3096314',
        'U3161339',
        'U3177572',
        'U3235760',
        'U3240548',
        'U3334405',
        'U3412485',
        'U3580399',
        'U4159204');

SELECT * FROM temp1;


SELECT b.*, a.StatusDisease FROM
    (SELECT PtMRN
        , PatientId
        , MIN(ComorbDate) AS SecondaryDate
        , MIN(Secondary) AS Secondary
        , SecondaryType
        FROM temp1
        GROUP  BY PtMRN, SecondaryType) b
        LEFT JOIN
    (SELECT PtMRN, StatusDisease FROM caisis.vdatasetstatus WHERE StatusDisease NOT LIKE '%aml%' GROUP BY PtMRN, StatusDisease) a
        ON a.PtMRN = b.PtMRN
    WHERE b.PtMRN IS NOT NULL ;










