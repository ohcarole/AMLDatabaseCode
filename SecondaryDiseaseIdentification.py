from Utilities.MySQLdbUtils import *
reload(sys)

sys.setdefaultencoding('utf8')

def Create_Secondary():
    """
    Code for this program originally came from the sql script:
    J:\Estey_AML\AML Programming\Python\sharedUtils\Secondary Disease Identification.sql
    :return:
    """
    cnxdict = connect_to_mysql_db_prod('temp')
    print('Recreating table secondarystatus')
    cnxdict['sql'] = """
        /*

        This file needs documented.  The effort here is to figure out from the various pieces of information I have
        whether or not a patient had secondary disease, and if so what the source of the secondary disease was

        */


        DROP TABLE IF EXISTS t_firstAML ;
        CREATE TEMPORARY TABLE t_firstAML
            SELECT ptmrn,  min(statusdate) as firstAMLstatusdate
                        FROM caisis.vdatasetstatus
                            WHERE (StatusDisease LIKE '%aml%' OR StatusDisease LIKE '%sarc%')
                                OR (StatusDisease LIKE '%mds%' AND Status LIKE '%work%')
                                OR (StatusDisease LIKE '%raeb%' AND Status LIKE '%work%')
                            GROUP BY ptmrn ;

        DROP TABLE IF EXISTS t_firststatus_1 ;
        CREATE TEMPORARY TABLE t_firststatus_1
            SELECT CASE
                        WHEN a.firstAMLstatusdate=b.statusdate THEN 'Yes'
                        ELSE ''
                    END AS FirstAMLStatus,
                    a.firstAMLstatusdate, b.*
                    FROM t_firstAML a
                LEFT JOIN caisis.vdatasetstatus b
                    ON a.ptmrn = b.ptmrn ; -- AND a.firststatusdate = b.statusdate ;

        DROP TABLE IF EXISTS t_firststatus ;
        CREATE TEMPORARY TABLE t_firststatus
            SELECT b.firstAMLstatusdate, a.* FROM caisis.vdatasetstatus a
                LEFT JOIN t_firststatus_1 b on b.ptmrn = a.ptmrn AND a.statusdate = b.firstAMLstatusdate
            GROUP BY a.ptmrn;

        DROP TABLE IF EXISTS   t_nd_arrivals ;
        CREATE TEMPORARY TABLE t_nd_arrivals
            SELECT ptmrn, statusdisease, min(statusdate) as statusdate FROM caisis.vdatasetstatus
            WHERE StatusDisease RLIKE 'AML.*ND[0-9]?'
            GROUP BY ptmrn, statusdisease;

        DROP TABLE IF EXISTS   t_rr_arrivals ;
        CREATE TEMPORARY TABLE t_rr_arrivals
            SELECT c.statusdate as statusdate_
                , c.StatusDisease as statusdisease_
                , c.status as status_
                , a.ptmrn
                , a.statusdate as FirstStatus
                , b.statusdate as NDDate
                , a.statusdisease
            FROM caisis.vdatasetstatus c
                    LEFT JOIN t_firststatus a
                            ON a.ptmrn = c.ptmrn
                    LEFT JOIN t_nd_arrivals b
                            ON b.ptmrn = c.ptmrn
            WHERE a.StatusDisease RLIKE 'AML.*(REL|REF)'
            ORDER BY c.ptmrn, c.statusdate;

        DROP TABLE IF EXISTS   t_comorb ;
        CREATE TEMPORARY TABLE t_comorb
            SELECT * FROM caisis.vdatasetcomorbidities
                WHERE Comorbidity IN ('AHD','Chemotherapy History') ;

        DROP TABLE IF EXISTS   t_ahd ;
        CREATE TEMPORARY TABLE t_ahd
            SELECT * FROM caisis.vdatasetstatus
                WHERE Status LIKE '%antecedent%';


        # SELECT * FROM t_firstAML ORDER BY PtMRN;
        # SELECT * FROM t_firststatus_1 ORDER BY PtMRN, StatusDate;
        # SELECT * FROM t_firststatus ORDER BY PtMRN, StatusDate;
        # SELECT * FROM t_nd_arrivals ORDER BY PtMRN;
        # SELECT * FROM t_rr_arrivals ORDER BY PtMRN;
        # SELECT * FROM t_comorb;
        # SELECT * FROM t_ahd;
        # SELECT * FROM caisis.vdatasetstatus  ORDER BY PtMRN;

        DROP TABLE IF EXISTS temp.SecondaryStatus;
        CREATE TABLE temp.SecondaryStatus AS
        SELECT
            a.PtMRN,
            a.PatientId,
            a.PtFirstName,
            a.PtLastName,
            f.StatusDate as ArrivalDate,
            b.ComorbDate,
            b.Comorbidity,
            b.ComorbNotes,
            CASE
                WHEN c.StatusDisease IS NOT NULL THEN c.StatusDisease
                ELSE d.statusdisease
            END AS StatusDisease,
            d.StatusDisease as Status,
            CASE
                WHEN e.Status = 'Antecedent Hematologic Disorder' THEN 'Yes'
                WHEN b.Comorbidity = 'AHD' THEN 'Yes'
                WHEN b.Comorbidity = 'Chemotherapy History' THEN 'Yes'
                WHEN c.StatusDisease LIKE '%nd2%' AND c.StatusDisease LIKE '%aml%' THEN 'Yes'
                WHEN c.StatusDisease LIKE '%nd1%' AND c.StatusDisease LIKE '%aml%' THEN 'No'
                WHEN d.StatusDisease LIKE '%aml%' THEN 'Not documented'
                ELSE 'Not documented'
            END AS Secondary,
            CASE
                WHEN e.Status = 'Antecedent Hematologic Disorder' THEN concat('Documented AHD (type ', e.StatusDisease,')')
                WHEN b.Comorbidity = 'AHD' THEN 'AHD (type unknown)'
                WHEN b.Comorbidity = 'Chemotherapy History' THEN 'Chemotherapy History (type unknown)'
                WHEN c.StatusDisease LIKE '%nd2%' THEN 'Secondary AML (type unknown)'
                WHEN c.StatusDisease LIKE '%nd1%' AND c.StatusDisease LIKE '%aml%' THEN 'De Novo AML'
                WHEN d.StatusDisease LIKE '%aml%' THEN 'Uncertain, Hx of AHD/Chemo not recorded'
                ELSE 'Uncertain, Hx of AHD/Chemo not recorded'
            END AS SecondaryType
        FROM caisis.vdatasetstatus f
                LEFT JOIN caisis.vdatasetpatients a on a.ptmrn = f.ptmrn
                LEFT JOIN t_comorb b ON a.PtMRN = b.PtMRN
                LEFT JOIN t_nd_arrivals c ON a.PtMRN = c.PtMRN
                LEFT JOIN t_rr_arrivals d ON a.PtMRN = d.PtMRN
                LEFT JOIN t_ahd e ON a.PtMRN = e.PtMRN
        WHERE f.ptmrn is not null AND f.statusdisease NOT LIKE '%apl%';

        ALTER TABLE `temp`.`secondarystatus`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

        DROP TABLE IF EXISTS caisis.secondarystatus ;
        CREATE TABLE caisis.secondarystatus
            SELECT a.PatientId, a.PtMRN
                , GROUP_CONCAT(CASE
                    WHEN a.SecondaryType IS NULL THEN ''
                    ELSE a.SecondaryType
                END) AS SecondaryType
            FROM (SELECT PtMRN, PatientId
                    , SecondaryType
                FROM temp.secondarystatus
                GROUP BY PatientID, PtMRN, SecondaryType) a
            GROUP BY PatientId;

        ALTER TABLE `caisis`.`secondarystatus`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        /*

        This section returns the list of patients for which we do NOT know secondary status

        */

        DROP TABLE IF EXISTS t_SecondaryNotDocumented ;
        CREATE TEMPORARY TABLE t_SecondaryNotDocumented
            SELECT a.PtMRN, a.PtFirstName, a.PtLastName, min(b.statusdate) as FirstStatus
                FROM temp.temp1 a
                    LEFT JOIN caisis.vdatasetstatus b on a.ptmrn = b.ptmrn
                WHERE Secondary = 'Not documented'
                GROUP BY a.ptmrn, a.comorbdate, a.comorbidity
                ORDER BY FirstStatus;
        # SELECT * FROM t_SecondaryNotDocumented ;



        /*

        This section returns the list of patients for which we DO know secondary status
        And when possible the source of that knowledge

        */

        # SELECT a.PtMRN,
        #     a.PatientId,
        #     a.PtFirstName,
        #     a.PtLastName,
        #     a.ComorbDate,
        #     a.Comorbidity,
        #     a.ComorbNotes,
        #     a.StatusDisease,
        #     a.Status,
        #     a.Secondary,
        #     a.SecondaryType
        #     FROM temp.temp1 a
        #     WHERE Secondary <> 'Not documented'
        #     GROUP BY a.ptmrn, a.comorbdate, a.comorbidity;

    """
    try:
        dosqlexecute(cnxdict)  # normally do not need to recreate views
        print("Secondarystatus created")
    except:
        print("Failed to create secondarystatus")
    return


# Create_Secondary()
