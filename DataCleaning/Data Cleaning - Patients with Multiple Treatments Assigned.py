from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

cnxdict['sql'] = """
# Starting the effort to identify TRM for patients.  Issue is that we
# only want to count protocols that are actually induction or currative treatment.

/*
SELECT DISTINCT
    PtMRN
    , PatientId
    , MedTxDate
    , MedTxAgent
    , Categorized
    , Intensity
    , Wildcard
FROM ProtocolCategory.PatientProtocol ;

SELECT * FROM TEMP.FirstTxDate;

SELECT * FROM Caisis.`v_arrival_with_prev_next`;
*/


/*************************************************************************************
Recreates the table vdatasetarrivalwithprevnext
*/
call caisis.`Create Arrival with Prev and Next 20180125`();


/*************************************************************************************
Patients with an arrival date joined to view arrival with next in
order to find a range between which all relevant dates must fall
*/



DROP TABLE IF EXISTS temp.Result1 ;
CREATE TABLE temp.Result1
    SELECT a.PtMRN
        , a.PatientId
        , StatusDisease AS ArrivalDx # test comment
        , StatusDate AS ArrivalDate
        , YEAR(StatusDate) AS ArrivalYear
        , CAST(NULL AS DATETIME) AS TreatmentStartDate
        , CAST(NULL AS DATETIME) AS ResponseDate
        , CAST(NULL AS CHAR(15)) AS Response
        , CAST(NULL AS CHAR(40)) AS FlowSource
        , CAST(NULL AS DECIMAl(7,2)) AS FlowBlasts
        , b.NextArrivalDate
        , StatusDate AS TargetDate
        , StatusDate AS FirstRangeDate # 3 days before arrival
        , CASE # up to 100 days after arrival
            WHEN b.NextArrivalDate BETWEEN a.StatusDate AND DATE_ADD(a.StatusDate, INTERVAL 100 DAY) THEN b.NextArrivalDate
            WHEN b.NextArrivalDate > DATE_ADD(a.StatusDate, INTERVAL 100 DAY)                        THEN NULL
            WHEN b.NextArrivalDate IS NULL                                                           THEN NULL
            ELSE NULL
        END AS LastRangeDate
    FROM caisis.vdatasetstatus a
    JOIN caisis.vdatasetarrivalwithprevnext b ON a.PatientId = b.PatientId and a.StatusDate = b.ArrivalDate
    WHERE a.status like '%work%' 
    ;

/*
Records where the patient was not arriving for AML
*/
SELECT * FROM temp.Result1 
    WHERE ArrivalDx NOT LIKE '%aml%' 
    AND   ArrivalDx NOT LIKE '%mds%'
    ;

/*
Remove APL
*/
DELETE FROM temp.Result1 
WHERE
    ArrivalDx NOT LIKE '%aml%'
    AND ArrivalDx NOT LIKE '%mds%'
;




/*

select 3031 - 2957;
SELECT * FROM caisis.vdatasetstatus 
    WHERE status like '%work%' and patientid is not null 
    ORDER BY PtMRN, StatusDate ;
SELECT * FROM caisis.vdatasetarrivalwithprevnext 
    GROUP BY PtMRN ; # patients
SELECT * FROM caisis.vdatasetarrivalwithprevnext 
    GROUP BY PtMRN, ArrivalDate;
SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
    WHERE status like '%work%' GROUP BY PtMRN ; # patients
SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
    WHERE status like '%work%' GROUP BY PtMRN, StatusDate ; # arrivals
SELECT * FROM Temp.Result1 ;
SELECT * FROM Temp.Result1 WHERE ArrivalDx LIKE '%CYTO%';
                
SELECT * FROM caisis.vdatasetstatus a JOIN caisis.vdatasetarrivalwithprevnext b on a.PtMRN = b.PtMRN and a.StatusDate = b.ArrivalDate ;

SET @i:=0;
SET @j:=0;
SELECT * FROM 
(SELECT @i:=@i+1 as Id, a.* FROM caisis.vdatasetstatus a WHERE status like '%work%' and patientid is not null ORDER BY PtMRN, StatusDate ) a
    JOIN 
(SELECT @j:=@j+1 as Id, a.* FROM caisis.vdatasetarrivalwithprevnext a ORDER BY PtMRN, ArrivalDate ) b
on a.id = b.id and a.ptmrn = b.ptmrn and a.statusdate = b.arrivaldate ;


SELECT s.PtMRN, a.* FROM caisis.vdatasetstatus a 
    JOIN (SELECT a.PtMRN FROM (SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE status like '%work%' GROUP BY PtMRN, StatusDate ) a
            LEFT JOIN (SELECT PtMRN, ArrivalDate, COUNT(*) FROM caisis.vdatasetarrivalwithprevnext GROUP BY PtMRN, ArrivalDate) b
            ON a.PtMRN = b.PtMRN AND a.StatusDate = b.ArrivalDate
            WHERE b.PtMRN IS NULL) s 
        ON a.PtMRN = s.PtMRN
        WHERE a.status like '%work%' ;

SELECT s.PtMRN, a.* FROM caisis.vdatasetstatus a 
    JOIN (SELECT a.PtMRN FROM (SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE status like '%work%' GROUP BY PtMRN, StatusDate ) a
            RIGHT JOIN (SELECT PtMRN, ArrivalDate, COUNT(*) FROM caisis.vdatasetarrivalwithprevnext GROUP BY PtMRN, ArrivalDate) b
            ON a.PtMRN = b.PtMRN AND a.StatusDate = b.ArrivalDate
            WHERE a.PtMRN IS NULL) s 
        ON a.PtMRN = s.PtMRN
        WHERE a.status like '%work%' ;

SELECT * FROM temp.Result1;
*/

/*************************************************************************************
Find all treatments where a treatment was actually given
*/
DROP TABLE IF EXISTS Temp.Treatment ;
CREATE TABLE Temp.Treatment
    SELECT * FROM caisis.vdatasetmedicaltherapy
        where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
        and upper(medtxagent) not in ('HU'
            , 'HU/PALLIATIVE'
            , 'PALLIATIVE CARE'
            , 'NO TREATMENT'
            , 'HOSPICE'
            , 'UNKNOWN'
        )
        AND MedTxIntent NOT IN ( 'Consolidation Regimen'
            , 'Immunosuppressive Regimen'
            , 'Maintenance Regimen'
            , 'Other Regimen'
            , 'Palliative Regimen'
            , 'Consolidation'
            , 'Immunosuppressive'
            );


/*************************************************************************************
Find all treatments where no treatment was given
*/
DROP TABLE IF EXISTS Temp.NonTreatment ;
CREATE TABLE Temp.NonTreatment
    SELECT * FROM caisis.vdatasetmedicaltherapy
        where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
        and upper(medtxagent) in ('HU'
            , 'HU/PALLIATIVE'
            , 'PALLIATIVE CARE'
            , 'NO TREATMENT'
            , 'HOSPICE'
            , 'UNKNOWN'
            ) 
            OR MedTxIntent IN ( 'Consolidation Regimen'
            , 'Immunosuppressive Regimen'
            , 'Maintenance Regimen'
            , 'Other Regimen'
            , 'Palliative Regimen'
            , 'Consolidation'
            , 'Immunosuppressive'
            );

/*
SELECT * FROM temp.Treatment;
SELECT * FROM temp.NonTreatment;
*/


/*************************************************************************************
Create a table of all possible treatment dates that fall in the (arrival to
next arrival
Fill in treatment start date
*/
DROP TABLE IF EXISTS Temp.Result2 ;
CREATE TABLE Temp.Result2
    SELECT a.PtMRN
        , a.PatientId
        , a.ArrivalDx
        , CASE
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate
                 AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                 THEN 'In range and w/i 100 days'
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate
                THEN 'In range'
            WHEN b.MedTxIntent LIKE '%induct%'
                AND a.ArrivalDx LIKE '%nd%'
                AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                THEN 'Induction within 100 days of arrival'
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                THEN 'Within 100 days of arrival'
            ELSE 'No relevant treatments w/i 100 days and before the next arrival'
        END AS TreatmentRelevance
        , a.ArrivalDate
        , a.ArrivalYear
        , b.MedTxDate AS TreatmentStartDate
        , b.MedTxIntent
        , b.MedTxType
        , b.MedTxAgent AS OriginalMedTxAgent
        , LTRIM(RTRIM(UPPER(b.MedTxAgent))) AS MedTxAgent
        , LTRIM(RTRIM(UPPER(b.MedTxAgent))) AS MedTxAgentNoParen
        , LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAgent)))) AS FirstParen
        , LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAgent)))))  AS LastParen
        , CAST(NULL AS DATETIME) AS ResponseDate
        , CAST(NULL AS CHAR(15)) AS Response
        , CAST(NULL AS DATETIME) AS FlowDate
        , CAST(NULL AS CHAR(40)) AS FlowSource
        , CAST(NULL AS DECIMAl(7,2)) AS FlowBlasts
        , CAST(NULL AS CHAR(45)) AS RelapseType
        , CAST(NULL AS DATETIME) AS RelapseDate
        , CAST(NULL AS CHAR(45)) AS RelapseDisease

/*
        , Status AS RelapseType
        , StatusDate AS RelapseDate
        , StatusDisease AS RelapseDisease
        , StatusNotes AS RelapseNotes
*/

        , a.NextArrivalDate
        , a.TargetDate
        , a.FirstRangeDate
        , a.LastRangeDate
        , TIMESTAMPDIFF(DAY,a.FirstRangeDate,b.MedTxDate) AS DaysFromArrivaltoTreatment
        , 'Not First Treatment' AS FirstTreatment
    FROM Temp.Result1 a
    LEFT JOIN Temp.Treatment b
    ON a.PatientId = b.PatientId
        AND CASE
                WHEN timestampdiff(day,a.FirstRangeDate,a.LastRangeDate) < 101
                    AND b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate THEN TRUE
                WHEN a.LastRangeDate IS NULL
                    AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY) THEN TRUE
                WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate THEN TRUE
                ELSE FALSE
            END
        ORDER BY PtMRN, ArrivalDate, TreatmentStartDate;

UPDATE TEMP.Result2 
    SET MedTxAgentNoParen = LTRIM(RTRIM(REPLACE(MedTxAgent, SUBSTRING(MedTxAgent,FirstParen,LastParen-FirstParen+2),''))) ;


DROP TABLE IF EXISTS temp.RelevantTreatments ;
CREATE TABLE temp.RelevantTreatments
    SELECT a.PtMRN
        , b.MedTxIntent
        , b.MedTxType
        , b.MedTxAgent AS OriginalMedTxAgent
        , date_format(a.ArrivalDate,'%m/%d/%Y') as ArrivalDate
        , date_format(b.MedTxDate,'%m/%d/%Y') as MedTxDate
        , date_format(a.NextArrivalDate,'%m/%d/%Y') as NextArrivalDate
        , timestampdiff(DAY, a.ArrivalDate, a.NextArrivalDate) as DxtoNext
        , timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) as DxtoRx
        , timestampdiff(DAY, b.MedTxDate, a.NextArrivalDate) as RxtoNext
        , CASE 
            # No treatment found
            WHEN b.MedTxDate IS NULL 
                THEN 'No treatment found'
            
            # Treatment is before arrival
            WHEN timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) < 0
                THEN 'Before arrival'

            # Treatment is more than 100 days later
            WHEN timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) > 100
                THEN '>100 days after arrival'

            # Next arrival is within 100 days
            WHEN timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) <= 100
                THEN '<=100 days after arrival'
            ELSE 'Error'
        END AS TreatmentType
        
        , CASE 
            # No next arrival known
            WHEN a.NextArrivalDate IS NULL 
                THEN 'Next arrival missing'
            
            # Next arrival is more than 100 days later
            WHEN timestampdiff(DAY, a.ArrivalDate, a.NextArrivalDate) > 100
                THEN '>100 days after arrival'

            # Next arrival is within 100 days
            WHEN timestampdiff(DAY, a.ArrivalDate, a.NextArrivalDate) <= 100
                THEN '<=100 days after arrival'
            ELSE 'Error'
        END AS NextArrivalType
        FROM Temp.Result1 a
        LEFT JOIN Temp.Treatment b
        ON a.PatientId = b.PatientId 
        WHERE NOT timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) < 0
        AND NOT timestampdiff(DAY, a.ArrivalDate, b.MedTxDate) > 100
        AND ( timestampdiff(DAY, b.MedTxDate, a.NextArrivalDate) > 0 OR  a.NextArrivalDate IS NULL );

# More than one treatment potentially relevant
SELECT * FROM temp.RelevantTreatments a 
    JOIN ( SELECT PtMRN, ArrivalDate, COUNT(*) AS RelevantCount 
        FROM temp.RelevantTreatments 
        GROUP BY PtMRN, ArrivalDate 
        HAVING COUNT(*) > 1) b 
    ON a.PtMRN = b.PtMRN 
        AND a.ArrivalDate = b.ArrivalDate
    ORDER BY a.PtMRN, a.ArrivalDate ;

"""
dosqlexecute(cnxdict)  # normally do not need to recreate views


filedescription = 'Patient Status '
sqlcmd = """
SELECT PtMRN, MIN(arrivalDate) AS FirstArrivalDate
        , PtBirthdate
        , PtLastName
        , PtDeathDate
        , PtDeathType
        , LastStatusDate
        , LastStatusType
        , LastInformationDate
        FROM temp.FindLastContactDate
        GROUP BY 1;
"""


print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription), index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)