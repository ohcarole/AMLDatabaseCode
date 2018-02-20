import os

print(os.path.dirname(os.path.realpath(__file__)))
from Utilities.MySQLdbUtils import *
import sys

reload(sys)

cnxdict = connect_to_mysql_db_prod('utility')


def create_timeline(cnxdict):

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS caisis.patienttimeline ;
        CREATE TABLE caisis.patienttimeline
        SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            EventResult
        FROM temp.v_arrival_with_prev_next

        UNION SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            EventResult
        FROM temp.v_diagnosis


        UNION -- ENCOUNTER
        SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            space(800) AS EventResult
        FROM temp.v_encounter

        UNION -- TREATMENT/CYCLES
        SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            EventResult
        FROM temp.v_treatment

        UNION -- HCT
        SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            EventResult
        FROM temp.v_hct

        UNION -- RESPONSE
        SELECT PtMRN
            , PatientId
            , Event
            , EventDate
            , EventDescription
            , EventResult
        FROM temp.v_response
        WHERE PtMRN IS NOT NULL

        UNION -- RELAPSE
        SELECT PtMRN
            , PatientId
            , Event
            , EventDate
            , EventDescription
            , space(800) AS EventResult
        FROM temp.v_relapse
        WHERE PtMRN IS NOT NULL

        UNION -- DECEASED
        SELECT
            PtMRN,
            PatientId,
            Event,
            EventDate,
            EventDescription,
            space(800) AS EventResult
        FROM temp.v_deceased

        UNION -- ALIVE
        SELECT PtMRN
            , PatientId
            , Event
            , EventDate
            , EventDescription
            , space(800) AS EventResult
        FROM temp.v_alive

        UNION -- MORPH
        SELECT PtMRN
            , PatientId
            , 'MORPH' as Event
            , dateobtained as EventDate
            , pathtest AS EventDescription
            , pathresult as EventResult
            from caisis.vdatasetpathtest
            where
                pathtest like '%blast%'
                and pathtest not like '%flow%'

        UNION -- FLOW
        SELECT PtMRN
            , PatientId
            , 'FLOW' as Event
            , dateobtained as EventDate
            , pathtest AS EventDescription
            , pathresult as EventResult
            from caisis.vdatasetpathtest
            where pathtest like '%flow%'

        UNION -- CYTO
        SELECT PtMRN
            , PatientId
            , 'CYTO' as Event
            , dateobtained as EventDate
            , CASE
                WHEN pathtest = '' THEN 'Cyto Karyotype'
                ELSE PathTest
            END AS EventDescription
            , pathresult as EventResult
            from temp.v_cyto

        UNION -- FISH
        SELECT PtMRN
            , PatientId
            , 'FISH' as Event
            , dateobtained as EventDate
            , CASE
                WHEN pathtest = '' THEN 'FISH'
                ELSE PathTest
            END AS EventDescription
            , pathresult as EventResult
            from temp.v_fish

        UNION -- CGAT
        SELECT PtMRN
            , PatientId
            , 'CGAT' as Event
            , dateobtained as EventDate
            , CASE
                WHEN pathtest = '' THEN 'CGAT'
                ELSE PathTest
            END AS EventDescription
            , pathresult as EventResult
            from temp.v_cgat

        ORDER BY PtMRN, EventDate, Event;

        ALTER TABLE caisis.patienttimeline
            ADD COLUMN `RecNum` INT(11) NOT NULL AUTO_INCREMENT FIRST,
            ADD PRIMARY KEY (`RecNum`),
            ADD COLUMN `PtName` VARCHAR(100) NULL AFTER `PatientId`,
            CHANGE COLUMN `Event` `Event` VARCHAR(50) ;

        ALTER TABLE `caisis`.`patienttimeline`
            ADD INDEX `PtName` (`PtName`(30) ASC) ,
            ADD INDEX `EventDate` (`EventDate` ASC) ,
            ADD INDEX `PatientId` (`PatientId` ASC) ,
            ADD INDEX `Event` (`Event`(30) ASC);

        UPDATE caisis.patienttimeline a, caisis.vdatasetpatients b
            SET PtName = concat(
                CASE
                    WHEN PtLastName IS NULL THEN ''
                    ELSE concat(PtLastName,', ')
                END
                ,
                CASE
                    WHEN PtFirstname IS NULL THEN ''
                    ELSE concat(PtFirstname,' ')
                END,
                        CASE
                    WHEN PtMiddlename IS NULL THEN ''
                    ELSE concat(LEFT(RTRIM(UPPER(PtMiddleName)),1),'.')
                END )
                WHERE a.PtMRN = b.PtMRN ;
    """
    dosqlexecute(cnxdict)


def output_timeline(cnxdict):
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')
    df = pd.read_sql("""
        SELECT * FROM caisis.patienttimeline;
    """, cnxdict['cnx'])
    cleandataframe(df, 'EventResult')
    try:
        df.to_excel(writer, sheet_name='Patient Timeline', index=False)
        writer.save()
        writer.close()
    except:
        pass

#create_timeline(cnxdict)
output_timeline(cnxdict)
print(cnxdict['out_filepath'])
