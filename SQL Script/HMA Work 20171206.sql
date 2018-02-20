SELECT * FROM hma_20170721.hmapatients_20171030;
#SELECT * FROM caisis.secondarystatus WHERE PtMRN IN (SELECT PtMRN FROM hma_20170721.hmapatients_20171030);
#SELECT * FROM caisis.vdatasetpatients WHERE PtMRN IN (SELECT PtMRN FROM hma_20170721.hmapatients_20171030);
#SELECT * FROM caisis.vdatasetmedicaltherapy 
#    WHERE PtMRN IN (SELECT PtMRN FROM hma_20170721.hmapatients_20171030);
/*
SELECT MedTxDisease
    , SUM(CASE
        WHEN MedTxType LIKE '%chemo%' 
            AND (MedTxDisease LIKE '%nhl%'
                OR MedTxDisease LIKE '%hodgkins%'
                OR MedTxDisease LIKE '%cmml%'
                OR MedTxDisease LIKE '%cll%'
                OR MedTxDisease LIKE '%all%'
                OR MedTxDisease LIKE '%cml%'
                OR MedTxDisease RLIKE 'mf\/?\s?'
                OR MedTxDisease RLIKE 'mm\/?\s?'
                OR MedTxDisease LIKE '%/et'
                OR MedTxDisease LIKE '%/pv'
                OR MedTxDisease LIKE '%/itp'
                OR MedTxDisease LIKE 'mpn%'
                OR MedTxDisease LIKE '%mds%'
                OR MedTxDisease LIKE '%sarcoma%'
                OR MedTxDisease LIKE '%apl%'
                OR MedTxDisease LIKE '%raeb%'
                OR MedTxDisease LIKE '%rars%'
                OR MedTxDisease LIKE '%rcmd%'
                OR MedTxDisease LIKE '%cyto%'
                OR MedTxDisease LIKE '%aml%')
            THEN 1
        ELSE NULL
    END) AS `Blood Cancer`
    , SUM(CASE
        WHEN MedTxType LIKE '%chemo%' 
            AND NOT (MedTxDisease LIKE '%nhl%'
                OR MedTxDisease LIKE '%hodgkins%'
                OR MedTxDisease LIKE '%cmml%'
                OR MedTxDisease LIKE '%cll%'
                OR MedTxDisease LIKE '%all%'
                OR MedTxDisease LIKE '%cml%'
                OR MedTxDisease RLIKE 'mf\/?\s?'
                OR MedTxDisease RLIKE 'mm\/?\s?'
                OR MedTxDisease LIKE '%/et'
                OR MedTxDisease LIKE '%/pv'
                OR MedTxDisease LIKE '%/itp'
                OR MedTxDisease LIKE 'mpn%'
                OR MedTxDisease LIKE '%mds%'
                OR MedTxDisease LIKE '%sarcoma%'
                OR MedTxDisease LIKE '%apl%'
                OR MedTxDisease LIKE '%raeb%'
                OR MedTxDisease LIKE '%rars%'
                OR MedTxDisease LIKE '%rcmd%'
                OR MedTxDisease LIKE '%cyto%'
                OR MedTxDisease LIKE '%aml%')
            THEN 1
        ELSE NULL
    END) AS `Non-Blood Cancer`
    FROM caisis.vdatasetmedicaltherapy 
    WHERE PtMRN IN (SELECT UWID FROM hma_20170721.hmapatients_20171030)
    GROUP BY MedTxDisease
    ORDER BY `Non-Blood Cancer` DESC, `Blood Cancer` DESC;

SELECT PtMRN, MedTxDate, MedTxType, MedTxDisease 
    FROM caisis.vdatasetmedicaltherapy 
    WHERE PtMRN IN (SELECT UWID FROM hma_20170721.hmapatients_20171030) ;
*/

SELECT * FROM caisis.vdatasetstatus WHERE status like '%ahd%';

DROP TABLE IF EXISTS Temp.junk ;
CREATE TABLE Temp.junk
SELECT DISTINCT uwid as PtMRN
    , 'Yes' AS `HMA Ever`
    , `HMA` AS `HMA Type`
    ,CASE
        WHEN a.VidazaCycles IS NULL  THEN a.DacogenCycles
        WHEN a.DacogenCycles IS NULL THEN a.VidazaCycles
        ELSE a.VidazaCycles + a.DacogenCycles
    END as `HMA Cycles`
    , `HMA Start`
    , `Response to HMA`
    , space(3) AS `AHD`
    , space(50) AS `AHD Type`
    , space(3) AS `Prior Chemo`
    , space(100) AS `Prior Chemo Type`
    , CAST(NULL AS DATETIME) AS `Prior Chemo Date`
    , -99 AS Age

     , space(100) AS `Intense Treatment`
     , a.protocol AS spreadsheetprotocol  # protocol from excel
     , space(100) AS `Treatment Add-On`
     , space(20)  AS `Treatment Intensity`

    , a.`treatment startdate` AS InductionStart
    , space(20) AS `TRM at Intense Treatment`
    , CAST(NULL AS SIGNED) as ECOG
    
    , space(100) AS risk
   
    , space(100) as `All Prev Chemo List`
    
    , CASE 
        WHEN a.VidazaStop IS NULL  THEN a.DacogenStop
        WHEN a.DacogenStop IS NULL THEN a.VidazaStop
        WHEN a.VidazaStop > a.DacogenStop THEN a.VidazaStop
        WHEN a.VidazaStop < a.DacogenStop THEN a.DacogenStop
        ELSE cast(null as datetime)
    END AS `HMA Stop`

    , CASE
        WHEN `HMA` = 'Vidaza' AND a.VidazaCycles IS NULL THEN 'Vidaza Cycles Missing'
        WHEN `HMA` = 'Both'   AND a.VidazaCycles IS NULL THEN 'Vidaza Cycles Missing'
        ELSE ''
    END AS VidazaCycleNote
    , a.VidazaCycles
    , a.DacogenCycles
    #, b.treatmentdate
    , timestampdiff(day,CASE 
        WHEN a.VidazaStop IS NULL  THEN a.DacogenStop
        WHEN a.DacogenStop IS NULL THEN a.VidazaStop
        WHEN a.VidazaStop > a.DacogenStop THEN a.VidazaStop
        WHEN a.VidazaStop < a.DacogenStop THEN a.DacogenStop
        ELSE cast(null as datetime)
    END,a.`treatment startdate`) AS `Days HMA to Induction`
    FROM hma_20170721.hmapatients_20171030 a
#     LEFT JOIN caisis.treatment b on a.uwid = b.ptmrn 
#         AND year(a.`treatment startdate`) = year(b.treatmentdate) 
#         AND month(a.`treatment startdate`) = month(b.treatmentdate)
#     LEFT JOIN caisis.backbonemapping3 c on (b.protocol = c.originalprotocol)
    ;

DROP TABLE IF EXISTS temp.junk_backbone ;
CREATE TABLE Temp.junk_backbone
SELECT a.PtMRN, InductionStart
        , a.spreadsheetprotocol
        , b.treatmentdate
        , b.protocol as caisistreatmentprotocol
        , c.originalprotocol
        , c.backbonename
        , c.anthracyclin
        , c.anthracyclindose
        , c.backboneaddon
        , c.intensity
    FROM temp.junk a
    LEFT JOIN caisis.treatment b ON a.ptmrn = b.ptmrn 
        AND year(a.InductionStart) = year(b.treatmentdate) 
        AND month(a.InductionStart) = month(b.treatmentdate)
    LEFT JOIN caisis.backbonemapping3 c ON (UPPER(b.protocol) = upper(c.originalprotocol)) ;

Update temp.junk a, temp.junk_backbone b
    SET a.`Intense Treatment` = b.backbonename
     , a.`Treatment Add-On` = b.backboneaddon
     , a.`Treatment Intensity` = left(b.intensity,20)
    WHERE a.ptmrn = b.ptmrn
        AND year(a.InductionStart) = year(b.treatmentdate) 
        AND month(a.InductionStart) = month(b.treatmentdate) ;

select * from temp.junk_backbone;

select * from temp.junk;

UPDATE Temp.junk SET risk = '';

# Add in fields of type text
alter table temp.junk add column Karyotype text AFTER ECOG;
alter table temp.junk add column KaryotypeDate datetime AFTER karyotype;
alter table temp.junk add column Karyotype_feed text AFTER KaryotypeDate;
alter table temp.junk add column KaryotypeDate_feed datetime AFTER Karyotype_feed;
alter table temp.junk add column flt3result text AFTER risk;
alter table temp.junk add column flt3ratio decimal(5,2) AFTER flt3result;
alter table temp.junk add column npm1result text AFTER flt3ratio;
alter table temp.junk add column cebparesult text AFTER npm1result;
alter table temp.junk add column hotspotdate datetime AFTER cebparesult;
alter table temp.junk add column hotspotpos text AFTER hotspotdate;
alter table temp.junk add column hotspotneg text AFTER hotspotpos;

alter table temp.junk add column HMAMorphBlastDate datetime AFTER hotspotneg;
alter table temp.junk add column HMAMorphBlastTest text AFTER HMAMorphBlastDate;
alter table temp.junk add column HMAMorphBlasts text AFTER HMAMorphBlastTest;
alter table temp.junk add column HMAFLOWBlastDate datetime AFTER HMAMorphBlasts;
alter table temp.junk add column HMAFLOWBlastTest text AFTER HMAFLOWBlastDate;
alter table temp.junk add column HMAFLOWBlasts text AFTER HMAFLOWBlastTest;
alter table temp.junk add column HMAPBMorphBlastDate datetime AFTER HMAFLOWBlasts;
alter table temp.junk add column HMAPBMorphBlastTest text AFTER HMAPBMorphBlastDate;
alter table temp.junk add column HMAPBMorphBlasts text AFTER HMAPBMorphBlastTest;
alter table temp.junk add column HMAPBFLOWBlastDate datetime AFTER HMAPBMorphBlasts;
alter table temp.junk add column HMAPBFLOWBlastTest text AFTER HMAPBFLOWBlastDate;
alter table temp.junk add column HMAPBFLOWBlasts text AFTER HMAPBFLOWBlastTest;


alter table temp.junk add column MorphBlastDate datetime AFTER HMAPBFLOWBlasts;
alter table temp.junk add column MorphBlastTest text AFTER MorphBlastDate;
alter table temp.junk add column MorphBlasts text AFTER MorphBlastTest;
alter table temp.junk add column FLOWBlastDate datetime AFTER MorphBlasts;
alter table temp.junk add column FLOWBlastTest text AFTER FLOWBlastDate;
alter table temp.junk add column FLOWBlasts text AFTER FLOWBlastTest;
alter table temp.junk add column PBMorphBlastDate datetime AFTER FLOWBlasts;
alter table temp.junk add column PBMorphBlastTest text AFTER PBMorphBlastDate;
alter table temp.junk add column PBMorphBlasts text AFTER PBMorphBlastTest;
alter table temp.junk add column PBFLOWBlastDate datetime AFTER PBMorphBlasts;
alter table temp.junk add column PBFLOWBlastTest text AFTER PBFLOWBlastDate;
alter table temp.junk add column PBFLOWBlasts text AFTER PBFLOWBlastTest;

alter table temp.junk add column Response text AFTER PBFLOWBlasts;
alter table temp.junk add column ResponseDate datetime AFTER Response;
SELECT secondarytype, count(*) FROM caisis.secondarystatus group by 1;
    
UPDATE temp.junk a, caisis.secondarystatus b
    SET a.`AHD Type` = CASE
        WHEN b.SecondaryType = 'Chemotherapy History (type unknown)' THEN ''
        WHEN b.SecondaryType LIKE '%De Novo%' THEN ''
        WHEN b.SecondaryType LIKE '%AHD (type RAEB-1%' THEN 'RAEB-1'
        WHEN b.SecondaryType LIKE '%AHD (type RAEB-2%' THEN 'RAEB-2'
        WHEN b.SecondaryType LIKE '%AHD (type MDS/RAEB-1%' THEN 'RAEB-1'
        WHEN b.SecondaryType LIKE '%AHD (type MDS/RAEB-2%' THEN 'RAEB-2'
        WHEN b.SecondaryType LIKE '%AHD (type MDS%' THEN 'MDS'
        WHEN b.SecondaryType LIKE '%AHD (type MDS%' THEN 'MDS'
        WHEN b.SecondaryType LIKE '%AHD (type CML%' THEN 'CML'
        WHEN b.SecondaryType LIKE '%AHD (type CMML%' THEN 'CMML'
        WHEN b.SecondaryType LIKE '%AHD (type unknown%' THEN 'Unknown'
        WHEN b.SecondaryType LIKE '%uncertain, Hx of AHD%' THEN 'Unknown'
        ELSE LEFT(b.SecondaryType,15)
    END
    , a.`AHD` = CASE
        WHEN b.SecondaryType LIKE '%AHD%' THEN 'Yes'
        WHEN b.SecondaryType LIKE '%De Novo%' THEN 'No'
        WHEN b.SecondaryType = 'Chemotherapy History (type unknown)' THEN 'No'
        ELSE '?'
    END
    , a.`Prior Chemo` = CASE
        WHEN b.SecondaryType LIKE '%chemo%' THEN 'Yes'
        ELSE ''
    END
    , a.`Prior Chemo Type` = CASE
        WHEN b.SecondaryType LIKE '%Chemotherapy History (type unknown)%' THEN 'Unknown'
        ELSE ''
    END
    WHERE a.PtMRN = b.PtMRN ;
    
UPDATE temp.junk a, caisis.vdatasetpatients b
    SET Age = timestampdiff(year,b.PtBirthDate,a.`InductionStart`)
    WHERE a.PtMRN = b.PtMRN ;


UPDATE temp.junk SET `All Prev Chemo List` = '';

UPDATE temp.junk a, (SELECT * FROM caisis.vdatasetstatus WHERE status like '%ahd%') b
    SET `AHD Type` = b.StatusDisease
    WHERE a.ptmrn = b.ptmrn;

UPDATE temp.junk a, (SELECT * FROM caisis.vdatasetstatus WHERE status like '%antecedent%') b
    SET `AHD Type` = b.StatusDisease
    WHERE a.ptmrn = b.ptmrn
    AND `AHD Type` = 'Unknown';



/*

In this section pull in all the documented cancers preceeding AML diagnosis

*/


# 
# UPDATE temp.junk a
#     , caisis.vdatasetmedicaltherapy b
#     SET `All Prev Chemo List` = concat(`All Prev Chemo List`,', ',b.MedTxDisease)
#     WHERE a.PtMRN = b.PtMRN 
#         AND b.MedTxDate < a.`InductionStart`
#         AND NOT LOCATE(b.MedTxDisease,`All Prev Chemo List`) > 1
#         AND b.MedTxDisease NOT LIKE '%mds%'
#         AND b.MedTxDisease NOT LIKE '%aml%';

DROP TABLE IF EXISTS temp.junk_prevchemo ;
CREATE TABLE temp.junk_prevchemo
SELECT 
    a.PtMRN, GROUP_CONCAT(concat(b.MedTxDisease,' [',date_format(b.MedTxDate,'%m/%Y'),']')) AS `Prev Chemo Disease`
FROM
    temp.junk a
        LEFT JOIN
    caisis.vdatasetmedicaltherapy b ON a.PtMRN = b.PtMRN
    WHERE a.InductionStart > b.MedTxDate 
        AND b.MedTxDisease NOT LIKE '%mds%'
        AND b.MedTxDisease NOT LIKE '%raeb%'
        AND b.MedTxDisease NOT LIKE '%aml%'
        AND b.MedTxDisease NOT LIKE '%ahd%'
        group by a.PtMRN;

UPDATE temp.junk a, temp.junk_prevchemo b
    SET a.`All Prev Chemo List` = b.`Prev Chemo Disease`
    WHERE a.PtMRN = b.PtMRN;


DROP TABLE IF EXISTS temp.junk_1;
CREATE TABLE temp.junk_1 
SELECT a.PtMRN, MedTxType, MedTxDate, MedTxDisease FROM caisis.vdatasetmedicaltherapy a 
    LEFT JOIN temp.junk b 
        ON a.PtMRN = b.PtMRN
    WHERE b.PtMRN IS NOT NULL
    AND a.MedTxDate < b.`InductionStart`
    AND a.MedTxDisease NOT RLIKE 'AML' 
    and a.MedTxType LIKE '%chemo%';

# SELECT * FROM temp.junk_1;

UPDATE temp.junk a
    , temp.junk_1 b
    SET `Prior Chemo Type` = b.MedTxDisease
    , `Prior Chemo` = 'Yes'
    , `Prior Chemo Date` = b.MedTxDate
    WHERE a.PtMRN = b.PtMRN 
        AND b.MedTxDate < a.`InductionStart`
        AND b.MedTxDisease NOT LIKE '%raeb%'
        AND b.MedTxDisease NOT LIKE '%/et%'
        AND b.MedTxDisease NOT LIKE '%mds%';


SELECT * FROM caisis.arrivaltrm;
        
UPDATE temp.junk b, caisis.arrivaltrm a
    SET b.`TRM at Intense Treatment` = a.`TRM_Version2 (Online)`
    , b.ECOG = a.ECOG
    WHERE a.PtMRN = b.PtMRN AND a.MedTxDate = b.InductionStart;

/*
Karyotype section
*/

DROP TABLE temp.junk_pathkaryo;
CREATE TABLE temp.junk_pathkaryo
    SELECT c.*, a.InductionStart FROM temp.junk a
        LEFT JOIN caisis.vdatasetpathology b
            ON a.ptmrn = b.ptmrn
        LEFT JOIN caisis.vdatasetpathtest c
            on b.PathologyId = c.PathologyId
            where c.pathkaryotype is not null
            AND c.pathtest like '%cyto%'
            AND c.dateobtained < a.InductionStart
            AND timestampdiff(day,c.dateobtained,a.Inductionstart) Between 0 AND 60;


UPDATE temp.junk a, temp.junk_pathkaryo b
    set a.karyotype = b.pathkaryotype
    , a.karyotypeDate = b.dateobtained
    where a.ptmrn = b.ptmrn and a.inductionstart = b.inductionstart ;



DROP TABLE IF EXISTS Temp.junk_karyotype;
CREATE TABLE Temp.junk_karyotype
SELECT a.PtMRN
    , GROUP_CONCAT(b.Type)
    , b.dateobtained
    , a.InductionStart
    , timestampdiff(day,b.dateobtained,a.Inductionstart)
    , GROUP_CONCAT(b.Pathtest)
    , b.pathresult
FROM
    temp.junk a
        LEFT JOIN
    caisis.allkaryo b ON a.ptmrn = b.ptmrn
WHERE
    b.PathTest LIKE '%karyo%'
        AND b.PathTest <> 'UW CYTO'
        AND b.PathTest NOT LIKE '%FISH%'
        AND b.dateobtained < a.InductionStart
        AND timestampdiff(day,b.dateobtained,a.Inductionstart) Between 0 AND 60
        AND b.pathresult > ''
        AND b.pathresult not rlike '\D.*'
GROUP BY PtMRN , dateobtained;

UPDATE temp.junk a, temp.junk_karyotype b
    set a.karyotype_feed = b.pathresult
    , a.karyotypeDate_feed = b.dateobtained
    where a.ptmrn = b.ptmrn and a.inductionstart = b.inductionstart ;

UPDATE temp.junk set risk = '' ;
UPDATE temp.junk set risk = concat(risk,', t(8;21)')      where karyotype like  '%t(8;21)%';
UPDATE temp.junk set risk = concat(risk,', t(15;17)')     where karyotype like  '%t(15;17)%';
UPDATE temp.junk set risk = concat(risk,', inv(16)')      where karyotype like  '%inv(16)%';

UPDATE temp.junk set risk = concat(risk,', monosomy 5')   where karyotype like  '%,-5%';
UPDATE temp.junk set risk = concat(risk,', monosomy 7')   where karyotype like  '%,-7%';
UPDATE temp.junk set risk = concat(risk,', del 5(q)')     where karyotype rlike 'del.{1}5';
UPDATE temp.junk set risk = concat(risk,', del 7(q)')     where karyotype rlike 'del.{1}7';
UPDATE temp.junk set risk = concat(risk,', 3q')           where karyotype rlike 'inv.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where karyotype rlike 'abn.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where karyotype rlike 'add.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where karyotype rlike 'del.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where karyotype rlike 'inv.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where karyotype rlike 'abn.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where karyotype rlike 'add.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where karyotype rlike 'del.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 17p')          where karyotype rlike 'abn.{1}17.*p';
UPDATE temp.junk set risk = concat(risk,', t(9;22)')      where karyotype like  '%t(9;22)%';
UPDATE temp.junk set risk = concat(risk,', t(6;9)')       where karyotype like  '%t(6;9)%';

UPDATE temp.junk set risk = concat(risk,', normal')       where karyotype rlike '46,X[XY].20';
UPDATE temp.junk set risk = concat(risk,', trisomy 8')    where karyotype like  '%+8%';
   
UPDATE temp.junk set risk = concat(risk,', t(8;21)')      where risk not like '%t(8;21)%'    and karyotype_feed like  '%t(8;21)%';
UPDATE temp.junk set risk = concat(risk,', t(15;17)')     where risk not like '%t(15;17)%'   and karyotype_feed like  '%t(15;17)%';
UPDATE temp.junk set risk = concat(risk,', inv(16)')      where risk not like '%inv(16)%'    and karyotype_feed like  '%inv(16)%';

UPDATE temp.junk set risk = concat(risk,', monosomy 5')   where risk not like '%monosomy 5%' and karyotype_feed like  '%,-5%';
UPDATE temp.junk set risk = concat(risk,', monosomy 7')   where risk not like '%monosomy 7%' and karyotype_feed like  '%,-7%';
UPDATE temp.junk set risk = concat(risk,', del 5(q)')     where risk not like '%del 5(q)%'   and karyotype_feed rlike 'del.{1}5';
UPDATE temp.junk set risk = concat(risk,', del 7(q)')     where risk not like '%del 7(q)%'   and karyotype_feed rlike 'del.{1}7';
UPDATE temp.junk set risk = concat(risk,', 3q')           where risk not like '%3q%'         and karyotype_feed rlike 'inv.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where risk not like '%3q%'         and karyotype_feed rlike 'abn.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where risk not like '%3q%'         and karyotype_feed rlike 'add.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 3q')           where risk not like '%3q%'         and karyotype_feed rlike 'del.{1}3.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where risk not like '%11q%'        and karyotype_feed rlike 'inv.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where risk not like '%11q%'        and karyotype_feed rlike 'abn.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where risk not like '%11q%'        and karyotype_feed rlike 'add.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 11q')          where risk not like '%11q%'        and karyotype_feed rlike 'del.{1}11.*q';
UPDATE temp.junk set risk = concat(risk,', 17p')          where risk not like '%17p%'        and karyotype_feed rlike 'abn.{1}17.*p';
UPDATE temp.junk set risk = concat(risk,', t(9;22)')      where risk not like '%t(9;22)%'    and karyotype_feed like  '%t(9;22)%';
UPDATE temp.junk set risk = concat(risk,', t(6;9)')       where risk not like '%t(6;9)%'     and karyotype_feed like  '%t(6;9)%';

UPDATE temp.junk set risk = concat(risk,', normal')       where risk not like '%normal%'     and karyotype_feed rlike '46,X[XY].20';
UPDATE temp.junk set risk = concat(risk,', trisomy 8')    where risk not like '%trisomy 8%'  and karyotype_feed like  '%+8%';

DROP TABLE IF EXISTS temp.junk_mole ;
CREATE TABLE temp.junk_mole
SELECT * FROM (SELECT labtestid, a.ptmrn, labdate, labtest, labresult
FROM
    temp.junk a
        LEFT JOIN
    caisis.mutation b ON a.ptmrn = b.ptmrn
union 
SELECT labtestid, a.ptmrn, labdate, labtest, labresult
FROM
    temp.junk a
        LEFT JOIN
    caisis.molecular c ON a.ptmrn = c.ptmrn) e
    group by labtestid, labtest, labresult;


SELECT * from caisis.vdatasetlabtests



update temp.junk a
    , (select * from temp.junk_mole where labtest = 'FLT3 Result') b
    SET a.flt3result = b.labresult
    where a.ptmrn = b.ptmrn
    and a.inductionstart >= b.labdate;

update temp.junk a
    , (select * from temp.junk_mole where labtest = 'FLT3 ITD to Normal Ratio') b
    SET a.flt3ratio = b.labresult
    where a.ptmrn = b.ptmrn
    and a.inductionstart >= b.labdate;

update temp.junk a
    , (select * from temp.junk_mole where labtest = 'NPM1 Insertion Mutation Result') b
    SET a.npm1result = b.labresult
    where a.ptmrn = b.ptmrn
    and a.inductionstart >= b.labdate;

update temp.junk a
    , (select * from temp.junk_mole where labtest = 'CEBPA Mutations Screen Result') b
    SET a.cebparesult = b.labresult
    where a.ptmrn = b.ptmrn
    and a.inductionstart >= b.labdate;

update temp.junk a, caisis.hotspot b 
    SET hotspotpos = b.PositiveTestList
    , hotspotneg = b.NegativeTestList
    , hotspotdate = b.SpecimenReceivedDtTm
    WHERE a.PtMRN = b.PtMRN 
    AND b.SpecimenReceivedDtTm < a.InductionStart;

/*
Update blasts at HMA treatment
*/
DROP TABLE IF EXISTS temp.junk_hmablasts;
CREATE TABLE temp.junk_hmablasts
SELECT a.`HMA Start`, timestampdiff(day,b.DateObtained,a.`HMA Start`) as daystorx, b.* FROM temp.junk a left join caisis.vdatasetpathtest b
    on a.ptmrn = b.ptmrn
    AND b.DateObtained < a.`HMA Start`
    AND timestampdiff(day,b.DateObtained,a.`HMA Start`) < 30
    where pathresult not in ('ND','Unknown','N/A')
    and (pathtest like '%blast%'
    or   pathtest like '%evidence%')
    ORDER BY a.ptmrn, daystorx;
    
# blasts by morph
DROP TABLE IF EXISTS temp.junk_firstmorph;
CREATE TABLE temp.junk_firstmorph
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_hmablasts a
    WHERE pathtest LIKE '%blasts (morph)%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstmorph b
    SET HMAMorphBlastDate = b.DateObtained
    , HMAMorphBlastTest = b.PathTest
    , HMAMorphBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# blasts by flow
DROP TABLE IF EXISTS temp.junk_firstflow;
CREATE TABLE temp.junk_firstflow
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_hmablasts a
    WHERE pathtest LIKE '%blasts (flow)%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstflow b
    SET HMAFLOWBlastDate = b.DateObtained
    , HMAFLOWBlastTest = b.PathTest
    , HMAFLOWBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# PBMorph
DROP TABLE IF EXISTS temp.junk_firstPBMorph;
CREATE TABLE temp.junk_firstPBMorph
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_hmablasts a
    WHERE pathtest LIKE '%pb%'
        AND pathtest LIKE '%blasts%'
        AND pathtest NOT LIKE '%flow%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstPBMorph b
    SET HMAPBMorphBlastDate = b.DateObtained
    , HMAPBMorphBlastTest = b.PathTest
    , HMAPBMorphBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# PBFlow
DROP TABLE IF EXISTS temp.junk_firstPBFLOW;
CREATE TABLE temp.junk_firstPBFLOW
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_hmablasts a
    WHERE pathtest LIKE '%pbflow%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstPBFLOW b
    SET HMAPBFLOWBlastDate = b.DateObtained
    , HMAPBFLOWBlastTest = b.PathTest
    , HMAPBFLOWBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;


/*
Update blasts at induction arrival
*/

DROP TABLE IF EXISTS temp.junk_blasts;
CREATE TABLE temp.junk_blasts
SELECT a.InductionStart, timestampdiff(day,b.DateObtained,a.InductionStart) as daystorx, b.* FROM temp.junk a left join caisis.vdatasetpathtest b
    on a.ptmrn = b.ptmrn
    AND b.DateObtained < a.inductionStart
    AND timestampdiff(day,b.DateObtained,a.InductionStart) < 30
    where pathresult not in ('ND','Unknown','N/A')
    and (pathtest like '%blast%'
    or   pathtest like '%evidence%')
    ORDER BY a.ptmrn, daystorx;
    
# blasts by morph
DROP TABLE IF EXISTS temp.junk_firstmorph;
CREATE TABLE temp.junk_firstmorph
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_blasts a
    WHERE pathtest LIKE '%blasts (morph)%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstmorph b
    SET MorphBlastDate = b.DateObtained
    , MorphBlastTest = b.PathTest
    , MorphBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# blasts by flow
DROP TABLE IF EXISTS temp.junk_firstflow;
CREATE TABLE temp.junk_firstflow
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_blasts a
    WHERE pathtest LIKE '%blasts (flow)%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstflow b
    SET FLOWBlastDate = b.DateObtained
    , FLOWBlastTest = b.PathTest
    , FLOWBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# PBMorph
DROP TABLE IF EXISTS temp.junk_firstPBMorph;
CREATE TABLE temp.junk_firstPBMorph
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_blasts a
    WHERE pathtest LIKE '%pb%'
        AND pathtest LIKE '%blasts%'
        AND pathtest NOT LIKE '%flow%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstPBMorph b
    SET PBMorphBlastDate = b.DateObtained
    , PBMorphBlastTest = b.PathTest
    , PBMorphBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;

# PBFlow
DROP TABLE IF EXISTS temp.junk_firstPBFLOW;
CREATE TABLE temp.junk_firstPBFLOW
SELECT MIN(daystorx) AS closesttotreatment, a.*
    FROM temp.junk_blasts a
    WHERE pathtest LIKE '%pbflow%'
    GROUP BY PtMRN;

UPDATE temp.junk a, temp.junk_firstPBFLOW b
    SET PBFLOWBlastDate = b.DateObtained
    , PBFLOWBlastTest = b.PathTest
    , PBFLOWBlasts = b.PathResult
    WHERE a.PtMRN = b.PtMRN ;


/*Create Response Info*/

    # ---  RESPONSE  -------------------------------------------------------------------------------------------


    drop table if exists temp.arrival ;
    CREATE table temp.arrival AS
        SELECT -999 as recnum
            , b.FirstArrival
            , a.statusdate as arrivaldate
            , a.statusdisease as arrivaldx
            , a.*
        FROM caisis.vdatasetstatus a 
            LEFT JOIN (
                SELECT PtMRN
                    , MIN(StatusDate) as FirstArrival 
                    FROM caisis.vdatasetstatus 
                    WHERE Status like '%arrival work%'
                    GROUP BY PtMRN ) b
                ON a.PtMRN = b.PtMRN
        WHERE a.status like '%arrival work%'
        ORDER BY a.ptmrn, a.statusdate ;

    set @idx := 0;
    UPDATE temp.arrival a SET recnum = @idx:=@idx + 1 ;
    
    ALTER TABLE temp.arrival ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

    drop table if exists temp.response ;
    CREATE table temp.response AS
        SELECT -999 as recnum, a.*
        FROM caisis.vdatasetstatus a
            join caisis.v_responsetypes b
                ON b.status LIKE a.Status
        ORDER BY a.ptmrn, a.statusdate ;

    set @idx := 0;
    UPDATE temp.response a SET recnum = @idx:=@idx + 1 ;

    # SELECT * from temp.response1 where ptmrn = 'U2531816';

    ALTER TABLE temp.response
        CHANGE COLUMN `recnum` `recnum` BIGINT(21) NOT NULL ,
        ADD PRIMARY KEY (`recnum`);

    ALTER TABLE temp.response ADD INDEX `PtMRN` (`PtMRN`(10) ASC);


    drop table if exists caisis.response ;
    create table caisis.response
    select
        a.recnum
        , a.ptmrn
        , a.patientid

        , a.statusdisease as arrivaldx0
        , CAST(NULL as DateTime) arrivaldate0
        , a.statusdate as responsedate0
        , a.status as responsedescription0

        , b.statusdisease as arrivaldx1
        , CAST(NULL as DateTime) arrivaldate1
        , b.statusdate as responsedate1
        , b.status as responsedescription1

        , c.statusdisease as arrivaldx2
        , CAST(NULL as DateTime) arrivaldate2
        , c.statusdate as responsedate2
        , c.status as responsedescription2

        , d.statusdisease as arrivaldx3
        , CAST(NULL as DateTime) arrivaldate3
        , d.statusdate as responsedate3
        , d.status as responsedescription3

        , e.statusdisease as arrivaldx4
        , CAST(NULL as DateTime) arrivaldate4
        , e.statusdate as responsedate4
        , e.status as responsedescription4

        , f.statusdisease as arrivaldx5
        , CAST(NULL as DateTime) arrivaldate5
        , f.statusdate as responsedate5
        , f.status as responsedescription5

        , g.statusdisease as arrivaldx6
        , CAST(NULL as DateTime) arrivaldate6
        , g.statusdate as responsedate6
        , g.status as responsedescription6

        , h.statusdisease as arrivaldx7
        , CAST(NULL as DateTime) arrivaldate7
        , h.statusdate as responsedate7
        , h.status as responsedescription7

        from temp.response a
        left join temp.response b
            on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
        left join temp.response c
            on a.ptmrn = c.ptmrn and a.recnum+2 = c.recnum
        left join temp.response d
            on a.ptmrn = d.ptmrn and a.recnum+3 = d.recnum
        left join temp.response e
            on a.ptmrn = e.ptmrn and a.recnum+4 = e.recnum
        left join temp.response f
            on a.ptmrn = f.ptmrn and a.recnum+5 = f.recnum
        left join temp.response g
            on a.ptmrn = g.ptmrn and a.recnum+6 = g.recnum
        left join temp.response h
            on a.ptmrn = h.ptmrn and a.recnum+7 = h.recnum
        order by ptmrn, responsedate0;

update caisis.response a, temp.arrival b
    SET a.arrivaldate0 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx0 = b.arrivaldx
        AND b.ArrivalDate between b.FirstArrival and a.ResponseDate0;
        
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate1 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx1 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate0 and a.ResponseDate1;        
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate2 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx2 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate1 and a.ResponseDate2;  
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate3 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx3 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate2 and a.ResponseDate3;  
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate4 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx4 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate3 and a.ResponseDate4;  
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate5 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx4 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate4 and a.ResponseDate5;  
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate6 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx5 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate5 and a.ResponseDate6;  
        
update caisis.response a, temp.arrival b
    SET a.arrivaldate6 = b.ArrivalDate 
    where a.ptmrn = b.ptmrn AND a.arrivaldx6 = b.arrivaldx
        AND b.ArrivalDate between a.ResponseDate6 and a.ResponseDate7;  
        
# select * from temp.arrival;
# select * from caisis.response;
        
/*END Create Response Info*/


UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription0
    , a.responsedate = b.responsedate0
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate0 and responsedate0;

UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription1
    , a.responsedate = b.responsedate1
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate1 and responsedate1;
    
UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription2
    , a.responsedate = b.responsedate2
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate2 and responsedate2;

UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription3
    , a.responsedate = b.responsedate3
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate3 and responsedate3;

UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription4
    , a.responsedate = b.responsedate4
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate4 and responsedate4;
    
UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription5
    , a.responsedate = b.responsedate5
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate5 and responsedate5;
    
UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription6
    , a.responsedate = b.responsedate6
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate6 and responsedate6;    
    
UPDATE temp.junk a, caisis.response b
    SET a.Response = b.responsedescription7
    , a.responsedate = b.responsedate7
    WHERE a.PtMRN = b.PtMRN and a.InductionStart between arrivaldate7 and responsedate7;        
    
SELECT * FROM Temp.junk;
    