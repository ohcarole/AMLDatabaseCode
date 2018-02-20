SELECT * from `caisis`.`v_arrival_with_prev_next` ;
 
DROP TABLE IF EXISTS temp.t1 ;
CREATE TABLE temp.t1 
    SELECT CAST(NULL AS DATETIME) AS Cycle1Date
        , space(100) as Cycle1Name
        , CAST(NULL AS DATETIME) AS Cycle2Date
        , space(100) as Cycle2Name
        , CAST(NULL AS DATETIME) AS Cycle3Date
        , space(100) as Cycle3Name
        , CAST(NULL AS DATETIME) AS ResponseDate
        , space(100) as Response
        , a.*
        
        FROM `caisis`.`v_arrival_with_prev_next` a
        WHERE YEAR(a.arrivaldate) > 2007
        ORDER BY a.ptmrn , a.arrivaldate;

ALTER TABLE `temp`.t1 ADD INDEX `PtMRN` (`PtMRN`(10) ASC);
SELECT * FROM temp.t1;


/*
Find Response Date
*/
drop table if exists temp.response;
CREATE TABLE Temp.response SELECT * FROM caisis.v_response;
ALTER TABLE temp.response ADD INDEX `PtMRN` (`PtMRN`(10) ASC);


# Assume Response Date between Arrival and Next Arrival
UPDATE temp.t1 a, temp.response b
    SET a.ResponseDate = b.ResponseDate,
    a.Response = b.ResponseDescription
    WHERE a.PtMRN = b.PtMRN AND a.ResponseDate IS NULL AND 
    b.ResponseDate BETWEEN ArrivalDate AND NextArrivalDate;
UPDATE temp.t1 a, temp.response b
    SET a.ResponseDate = b.ResponseDate,
    a.Response = b.ResponseDescription
    WHERE a.PtMRN = b.PtMRN AND a.ResponseDate IS NULL AND 
    b.ResponseDate > ArrivalDate AND NextArrivalDate IS NULL;


/*
FIND the first (induction) cycle
*/
# Find the first (induction) cycle when between arrival and response
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.ResponseDate ;

# Find the first (induction) cycle when between arrival and next arrival
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate between a.ArrivalDate and a.NextArrivalDate ;

# Find the first (induction) cycle when after arrival and next arrival does not exist
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate > a.ArrivalDate and a.NextArrivalDate IS NULL;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate > a.ArrivalDate and a.NextArrivalDate IS NULL;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn and a.Cycle1Date IS NULL and
    b.medtxdate > a.ArrivalDate and a.NextArrivalDate IS NULL;


SELECT * from `caisis`.`vdatasetencounters` ;
SELECT * from `caisis`.`vdatasetstatus` ;
SELECT * from `caisis`.`v_arrival_with_prev_next` ;
SELECT * FROM temp.t1;
SELECT * FROM  temp.response;
SELECT PtMRN, ArrivalDate
    , Cycle1Date, Cycle1Name
    , Cycle2Date, Cycle2Name
    , ResponseDate, Response, NextArrivalDate FROM temp.t1;


/*
FIND the second cycle
*/
# Find the second cycle when between cycle 1 and response
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.ResponseDate ;

# Find the second cycle when between cycle 1 and next arrival
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and b.medtxdate < a.NextArrivalDate ;

# Find the second cycle when after cycle 1 and next arrival does not exist
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and a.NextArrivalDate IS NULL;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and a.NextArrivalDate IS NULL;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle2Date = b.MedTxDate
    , a.Cycle2Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND a.Cycle1Date IS NOT NULL AND  a.ResponseDate IS NULL AND  a.Cycle2Date IS NULL and
    b.medtxdate > a.Cycle1Date and a.NextArrivalDate IS NULL;



SELECT PtMRN, ArrivalDate
    , Cycle1Date, Cycle1Name
    , Cycle2Date, Cycle2Name
    , Cycle3Date, Cycle3Name
    , ResponseDate, Response, NextArrivalDate FROM temp.t1;
    
/*
FIND the second cycle
*/
# Find the third cycle when between cycle 2 and response
UPDATE Temp.t1 SET Cycle3Date = NULL, Cycle3Name = '';
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.ResponseDate IS NOT NULL AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.ResponseDate IS NOT NULL AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.ResponseDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.ResponseDate IS NOT NULL AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.ResponseDate ;

# Find the third cycle when between cycle 2 and next arrival
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.NextArrivalDate ;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle3Date = b.MedTxDate
    , a.Cycle3Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    b.medtxdate > a.Cycle2Date AND b.medtxdate < a.NextArrivalDate ;

# Find the third cycle when after cycle 2 and next arrival does not exist
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    a.NextArrivalDate IS NULL AND 
    b.medtxdate > a.Cycle2Date;
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    a.NextArrivalDate IS NULL AND 
    b.medtxdate > a.Cycle2Date;    
UPDATE Temp.t1 a, caisismedtherapy b
    SET a.Cycle1Date = b.MedTxDate
    , a.Cycle1Name = b.BackBoneName
    WHERE a.ptmrn = b.ptmrn AND 
    a.Cycle1Date IS NOT NULL AND  
    a.Cycle2Date IS NOT NULL AND 
    a.Cycle3Date IS NULL AND 
    a.ResponseDate IS NULL AND 
    a.NextArrivalDate IS NULL AND 
    b.medtxdate > a.Cycle2Date;

# One reason a paient may not be associated with caisismedtherapy is no record in caisismedtherapy

select * from temp.caisismedtherapy;  # example patient U0406340

SELECT ArrivalDate, Cycle1Date, Cycle1Name, t1.* FROM temp.t1;
    