import os

print(os.path.dirname(os.path.realpath(__file__)))
from Utilities.MySQLdbUtils import *
import sys

reload(sys)

"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('utility')

def RelevantHCT(cnxdict):
    cnxdict['sql'] = """
    drop table if exists temp.hct;
    create table temp.hct
        select a.PtMRN
            , a.PatientId
            , a.StatusDisease AS ArrivalDx
            , a.StatusDate as ArrivalDate
            , b.ProcDate as HCTDate
            , c. `HCT count`
            , Case
                WHEN ProcedureId IS NULL THEN 'No HCT found'
                WHEN ProcedureId IS NOT NULL AND statusdate < b.procdate THEN 'After this arrival'
                WHEN ProcedureId IS NOT NULL AND statusdate >= b.procdate THEN 'Before ths arrival'
                ELSE NULL
            end as `When was HCT?`
            , case
                WHEN ProcedureId IS NOT NULL and statusdate > b.procdate
                    THEN timestampdiff(DAY,b.procdate,statusdate)
                ELSE null
            end as `Number of days after HCT patient Arrived`
            , case
                WHEN ProcedureId IS NOT NULL and statusdate <= b.procdate
                    THEN timestampdiff(DAY,statusdate,b.procdate)
                ELSE null
            end as `Number of days after arrival patient had HCT`
            , case
                WHEN ProcedureId IS NOT NULL and statusdate > b.procdate
                    THEN timestampdiff(MONTH,b.procdate,statusdate)
                ELSE null
            end as `Number of months after HCT patient Arrived`
            , case
                WHEN ProcedureId IS NOT NULL and statusdate <= b.procdate
                    THEN timestampdiff(MONTH,statusdate,b.procdate)
                ELSE null
            end as `Number of months after arrival patient had HCT`
            from (
                    select * from caisis.vdatasetstatus
                        where
                            statusdate between '2008-01-01' and '2016-06-30'
                            and status like '%arrival%'
                ) a
            left join (
                    select * from `caisis`.`vdatasetprocedures`
                        where
                            ProcName in ('HCT')
                ) b on a.ptmrn = b.ptmrn
            left join (
                    select ptmrn
                        , procdate
                        , sum(iF(procedureid is null, 0, 1)) as `HCT count`
                        from `caisis`.`vdatasetprocedures`
                        where
                            ProcName in ('HCT')
                            group by ptmrn
            ) c on a.ptmrn = c.ptmrn
            ORDER BY PtMRN, b.ProcDate, StatusDate;

    -- select * from `caisis`.`vdatasethctproc` limit 10000;

    -- select * from `caisis`.`vdatasetprocedures` limit 10000;

    -- select procname, count(*) from `caisis`.`vdatasetprocedures` group by procname;

    """
    print (cnxdict['sql'])
    dosqlexecute(cnxdict)
    return


RelevantHCT(cnxdict)