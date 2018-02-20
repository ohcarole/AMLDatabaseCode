/*
This programs is a template for building foreign relationships to status table information
*/

DROP TABLE caisis.ArrivalIdAssignment ;
CREATE TABLE caisis.ArrivalIdAssignment 
    SELECT CAST(NULL AS UNSIGNED) AS ArrivalId
        , StatusId
        , PatientId
        , PtMRN
        , `index` 
        FROM caisis.vdatasetstatus
    WHERE
        status LIKE '%arrival work%'
        AND StatusId BETWEEN 16000 and 17001
    ORDER BY StatusId;

SET @id:=0;
UPDATE caisis.ArrivalIdAssignment SET ArrivalId = @id:=@id+1;
SELECT * FROM caisis.ArrivalIdAssignment ;

DROP TABLE IF EXISTS caisis.new_arrival;
CREATE TABLE caisis.new_arrival
    SELECT b.ArrivalId, a.* from caisis.vdatasetstatus a
        LEFT JOIN ArrivalIdAssignment b
        ON a.StatusId = b.StatusId
    WHERE status like '%arrival work%' 
    AND b.ArrivalId IS NULL;
SELECT * from caisis.new_arrival ;

SET @maxid = (SELECT MAX(ArrivalId) FROM caisis.ArrivalIdAssignment) ;
UPDATE caisis.new_arrival SET ArrivalId = @maxid:=@maxid+1 ;
SELECT * from caisis.new_arrival ;

SELECT CASE 
        WHEN ArrivalId IS NULL THEN @maxid:=@maxid+1 
        ELSE ArrivalId
    END AS ArrivalId
    , StatusId
    , PatientId
    , PtMRN
    , `index` 
    FROM caisis.new_arrival 
    ORDER BY StatusId;

INSERT INTO caisis.ArrivalIdAssignment (ArrivalId
    , StatusId
    , PatientId
    , PtMRN
    , `index`) 
    SELECT ArrivalId
    , StatusId
    , PatientId
    , PtMRN
    , `index` FROM caisis.new_arrival;
    
DROP TABLE IF EXISTS caisis.new_arrival;

DROP TABLE IF EXISTS caisis.arrival_;
CREATE TABLE caisis.arrival_
    SELECT b.ArrivalId, a.* from caisis.vdatasetstatus a
        LEFT JOIN ArrivalIdAssignment b
        ON a.StatusId = b.StatusId
    WHERE status like '%arrival work%' ;
    
SELECT * from caisis.arrival_;