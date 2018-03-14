from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'Example'
cnxdict['sql'] = """
CREATE TABLE PlaygroundDatabase.EEPatientList
    SELECT c.arrival_id
            , b.patientid
            , ptmrn_ee
            , ptlastname_ee
            , arrivaldate_ee
            , arrivalage_ee
            , performancestatus_ee
            , arrivaldx_ee
            , treatment_ee
            , various_ee
            , cyto_ee
            , cr1dur_ee
            , response_ee
            , comment_ee
    FROM
        temp.eepatientlist_20180221 a
            LEFT JOIN caisis.vdatasetpatients b ON b.ptmrn = a.ptmrn_ee
            LEFT JOIN caisis.arrivalidmapping c ON b.patientid = c.patientid
                AND a.arrivaldate_ee = c.arrivaldate
    ORDER BY a.Order; 
"""
cnxdict['sql'] = """
    # Patients with typo's in their mrn
    UPDATE  temp.eepatientlist_20180221
        SET ptmrn_ee = CASE
            WHEN ptmrn_ee = 'U284945'    THEN 'U2684945' -- Friesen
            WHEN ptmrn_ee = 'U733006'    THEN 'U2733006' -- Fobes 
            WHEN ptmrn_ee = 'U312274'    THEN 'U3122274' -- Lokis
            WHEN ptmrn_ee = 'U3802423`'  THEN 'U3802423' -- Epperson
            WHEN ptmrn_ee = 'U417858'    THEN 'U4172858' -- Tea
            WHEN ptmrn_ee = 'U4287687`'  THEN 'U4287687' -- Song
            WHEN ptmrn_ee = 'UI3127798'  THEN 'U3127798' -- Wareham
            WHEN ptmrn_ee = 'U3255585`'  THEN 'U3255585' -- Lee
            WHEN ptmrn_ee = 'U2686829'   THEN 'U2686689' -- Ngo
            WHEN ptmrn_ee = 'U3051884'   THEN 'U3047409' -- Bengtsson
            WHEN ptmrn_ee = 'U0188449'   THEN 'U0108449' -- Bainton
            WHEN ptmrn_ee = 'U3169948'   THEN 'U2060522' -- Rall
            WHEN ptmrn_ee = 'U3121382'   THEN 'U3211382' -- Roesberry
            WHEN ptmrn_ee = 'U2405066'   THEN 'U2405006' -- Lozier
            WHEN ptmrn_ee = 'U3366667'   THEN 'U3339053' -- Titialii
            WHEN ptmrn_ee = 'U3494132'   THEN 'U3494192' -- Lafferty
            WHEN ptmrn_ee = 'U3568064'   THEN 'U3568054' -- Harris
            WHEN ptmrn_ee = ' U3656935'  THEN 'U3656935' -- Light
            WHEN ptmrn_ee = 'U4377186'   THEN 'U4377196' -- Albrant
            WHEN ptmrn_ee = 'U3954152'   THEN 'U3754152' -- Skinner
            WHEN ptmrn_ee = 'U4224126'   THEN 'U4221426' -- Samms
            WHEN ptmrn_ee = 'U44463283'  THEN 'U4463283' -- Kellogg
            WHEN ptmrn_ee = 'U731490'    THEN 'U2731490' -- Martinez
            WHEN ptmrn_ee = ''  THEN '' --
            WHEN ptmrn_ee = ''  THEN '' --
            WHEN ptmrn_ee = ''  THEN '' --
            WHEN ptmrn_ee = ''  THEN '' --
            WHEN ptmrn_ee = ''  THEN '' --
            WHEN ptmrn_ee = ''  THEN '' --
            ELSE ptmrn_ee
        END
    ;
    
    
    # patients with typo's in their arrival diagnosis
    UPDATE  temp.eepatientlist_20180221
        SET arrivaldx_ee = CASE
            WHEN arrivaldx_ee = 'AMLREL' THEN 'AML REL' 
            ELSE arrivaldx_ee
        END
    ;
    
    # Patients missing arrival date
    UPDATE  temp.eepatientlist_20180221
        SET arrivaldate_ee = CASE
            WHEN arrivaldate_ee IS NULL and treatment_ee = '9019 off'   and ptmrn_ee = 'U3767840' THEN '2015-02-27' -- Galeai had missing arrival date
            WHEN arrivaldate_ee IS NULL and treatment_ee = '2690'       and ptmrn_ee = 'U3656935' THEN '2014-10-06' -- Light  had missing arrival date
            WHEN arrivaldate_ee IS NULL and treatment_ee = 'palliative' and ptmrn_ee = 'U2755083' THEN '2014-03-15' -- Mays   had missing arrival date
            ELSE arrivaldate_ee
        END
    ;
    
    
    
    DROP TABLE IF EXISTS PlaygroundDatabase.EEPatientList ;
    CREATE TABLE PlaygroundDatabase.EEPatientList
        SELECT    a.`order`
                , CASE
                    WHEN b.arrival_id IS NULL THEN NULL
                    WHEN a.arrivaldate_ee = b.arrivaldate THEN concat('perfect',space(25)) 
                    ELSE NULL
                END as matchtype_ee
                , b.arrival_id as arrival_id_ee
                , b.patientid
                , ptmrn_ee
                , ptlastname_ee
                , arrivaldate_ee
                , arrivalage_ee
                , performancestatus_ee
                , arrivaldx_ee
                , treatment_ee
                , various_ee
                , cyto_ee
                , cr1dur_ee
                , response_ee
                , comment_ee
        FROM
            temp.eepatientlist_20180221 a
                LEFT JOIN caisis.arrivalidmapping b ON a.ptmrn_ee = b.ptmrn 
        ORDER BY a.`order`;
    
       
    SELECT * FROM PlaygroundDatabase.EEPatientList WHERE arrival_id_ee is null;
    
    update playgrounddatabase.eepatientlist a, caisis.arrivalidmapping b
            set   a.matchtype_ee   = 'lastname and date'
                , a.arrival_id_ee  = b.arrival_id
            WHERE a.ptlastname_ee  = b.ptlastname 
            and   a.arrivaldate_ee = b.arrivaldate 
            and   a.arrival_id_ee IS NULL
            and   a.matchtype_ee IS NULL ;
    
    
    update playgrounddatabase.eepatientlist a, caisis.arrivalidmapping b
            set   a.matchtype_ee   = 'within 5 days'
                , a.arrival_id_ee  = b.arrival_id
            WHERE timestampdiff(day,a.arrivaldate_ee,b.arrivaldate) between -5 and 5 
            and   a.ptmrn_ee = b.ptmrn 
            and   a.arrival_id_ee IS NULL
            and   a.matchtype_ee IS NULL ;
    
    
    update playgrounddatabase.eepatientlist a, caisis.arrivalidmapping b
            set   a.matchtype_ee   = 'within 10 days'
                , a.arrival_id_ee  = b.arrival_id
            WHERE timestampdiff(day,a.arrivaldate_ee,b.arrivaldate) between -10 and 10
            and   a.ptmrn_ee = b.ptmrn 
            and   a.arrival_id_ee IS NULL
            and   a.matchtype_ee IS NULL ;
"""

dosqlexecute(cnxdict)


writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'


sqlcmd1 = """
SELECT * FROM PlaygroundDatabase.EEPatientList 
    WHERE arrival_id_ee IS NULL 
        and arrivaldx_ee not like '%apl%'
        and treatment_ee not like '%pall%'
"""
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))

sqlcmd2 = """
SELECT * FROM PlaygroundDatabase.EEPatientList 
    WHERE arrival_id_ee IS NOT NULL 
"""
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))


df = pd.read_sql(sqlcmd1, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet1'.format(filedescription), index=False)
df = pd.read_sql(sqlcmd2, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet2'.format(filedescription), index=False)
dowritersave(writer, cnxdict)
print(writer.path)

# for row in df.itertuples():
#     print(row)