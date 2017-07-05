from MySQLdbUtils import *

def create_karyolist(cnxdict,writer):
    """
    This program assumes that the temp.patientlist needs to have associated karyotypes
    :param cnxdict:
    :param writer:
    :return:
    """
    # For working on karyotype
    cnxdict['sql'] = """
        drop table if exists temp.t1;
        create table temp.t1
            select a.PatientId, a.PtMRN, a.ArrivalDate
                    , b.type
                    , CASE
                            WHEN b.`DateObtained` IS NULL
                                THEN STR_TO_DATE(b.PathDate, '%Y-%m-%d')
                            WHEN b.`DateObtained` = '1900-01-01'
                                    AND b.PathDate IS NOT NULL
                                THEN STR_TO_DATE(b.PathDate, '%Y-%m-%d')
                            ELSE STR_TO_DATE(b.DateObtained, '%Y-%m-%d')
                        END AS DateObtained
                    , CASE
                            WHEN b.`Karyo` = '' THEN b.PathResult
                            WHEN b.PathResult = '' THEN b.Karyo
                            ELSE NULL
                        END AS karyo
                    from temp.patientlist a
                left join caisis.allkaryo  b on a.ptmrn = b.ptmrn
                where ( b.karyo <> '' or pathresult <> '' )
                    and left(b.karyo,3)      <> 'nuc'
                    and left(b.pathresult,3) <> 'nuc';
        select * from temp.t1;

        drop table if exists temp.t2;
        create table temp.t2
            select 'A'
                    , PatientId
                    , PtMRN
                    , ArrivalDate
                    , max(dateobtained) as DateObtained
                    , Type
                    , Karyo
                from temp.t1
                where dateobtained <= arrivaldate
                group by patientid, arrivaldate
            union
            select 'B'
                    , PatientId
                    , PtMRN
                    , ArrivalDate
                    , min(dateobtained) as DateObtained
                    , Type
                    , Karyo
                from temp.t1
                where dateobtained > arrivaldate
                group by patientid, arrivaldate
            ORDER BY PatientId, arrivaldate, dateobtained ;
        alter table temp.t2 add id int primary key auto_increment;
        select * from temp.t2;

        drop table if exists temp.t3;
        create table temp.t3
        Select a.PatientId
                    , a.PtMRN
                    , a.ArrivalDate
                    , a.DateObtained
                    , b.karyo
            FROM (select min(id) as TempId
                        , PatientId
                        , PtMRN
                        , ArrivalDate
                        , min(DateObtained) as DateObtained
                    from temp.t2
                    group by PatientId, PtMRN, ArrivalDate ) a
            LEFT JOIN temp.t2 b
                on  a.PatientId = b.PatientId
                and a.DateObtained = b.DateObtained ;
    """
    dosqlexecute(cnxdict)

    sql = "select * from temp.t1;"
    df = dosqlread(sql,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='karyo history detail', index=False)

    sql = "select patientid, ptmrn, arrivaldate, dateobtained, karyo from temp.t3;"
    df = dosqlread(sql,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='karyosummary', index=False)
    return None

