from Utilities.MySQLdbUtils import *
reload(sys)

cnxdict = connect_to_mysql_db_prod('mrcrisk')


def create_unique_karyotypes(cnxdict):
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS `temp`.`failure`;

        CREATE TABLE `temp`.`failure` (
          `karyotypeid` int(11)       NULL DEFAULT NULL,
          `karyotype` varchar({0})    NULL DEFAULT NULL,
          `subkaryotype` varchar({0}) NULL DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        DROP TABLE IF EXISTS `temp`.`karyo_variation`;

        CREATE TABLE `temp`.`karyo_variation` (
          `karyotypeid` int(11) NOT NULL AUTO_INCREMENT,
          `karyotype_freq` int(5) NULL DEFAULT NULL,
          `karyotype` varchar({0}) NULL DEFAULT NULL,
          `subkaryotype_list` varchar(40) DEFAULT NULL,
          `clone_list` varchar(40) DEFAULT NULL,
          `abnormalclone_list` varchar(40) DEFAULT NULL,
          `clone_count` int(2) NULL DEFAULT NULL,
          `abnormalclone_count` int(2) NULL DEFAULT NULL,
          `is_misc` tinyint(1) NULL DEFAULT NULL,
            PRIMARY KEY (`karyotypeid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

        INSERT INTO `temp`.`karyo_variation` (karyotype_freq, karyotype)
                SELECT count(*) AS karyotype_freq
                    , CASE
                        WHEN length(pathresult)>{0} THEN concat('<TRUNCATED>',left(pathresult,{0}-11))
                        ELSE pathresult
                    END as karyotype
                    FROM `caisis`.`allkaryo`
                    WHERE LOWER(pathresult) NOT RLIKE '(inevaluable|please see comment|insufficient|n\/a|cancel|failed|unk|inadequate|inconclusive)'
                        and LOWER(pathresult) NOT RLIKE '(^not|^no[[:space:]]|nuc[[:space:]])'
                        and LTRIM(RTRIM(pathresult)) <> ''
                        and (`type` LIKE '%cyto%' OR `Pathtest` LIKE '%cyto%')
                    GROUP BY LTRIM(RTRIM(pathresult))
                    ORDER BY COUNT(*) DESC, LTRIM(pathresult);
    """.format('800')
    dosqlexecute(cnxdict)


def create_subkaryotype_table(cnxdict):
    """
    Calling this "subkaryotype" rather than clone because not all portions will meet the definition of clone and
    I don't want the confusion of a non-clone clone.
    :param cnxdict:
    :return:
    """
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS `temp`.`subkaryo_variation`;

        CREATE TABLE `temp`.`subkaryo_variation` (
                `subkaryotypeid`       int(11)      NOT NULL AUTO_INCREMENT,
                `subkaryotype`         varchar({0})     NULL DEFAULT NULL,
                `abnormalityid`        int(11)          NULL DEFAULT NULL,
                `abnormality_list`     varchar({1})          DEFAULT NULL,
                `abnormalclone_count`  int(2)           NULL DEFAULT NULL,
                `abnormality_count`    int(2)           NULL DEFAULT NULL,
                `karyotypeid`          int(11)               DEFAULT NULL,
                `chromosomes`          varchar(20)           DEFAULT NULL,
                `diploidity`           varchar(40)           DEFAULT NULL,
                `gender`               varchar(10)           DEFAULT NULL,
                `cells`                int(2)           NULL DEFAULT NULL,
                `metaphases`           varchar(10)      NULL DEFAULT NULL,
                `is_normal`            tinyint(1)       NULL DEFAULT NULL,
                `is_chimerism`         tinyint(1)       NULL DEFAULT NULL,
                `is_abnormal`          tinyint(1)       NULL DEFAULT NULL,
                `is_misc`              tinyint(1)       NULL DEFAULT NULL,
            PRIMARY KEY (`subkaryotypeid`)
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    """.format('800','500')
    dosqlexecute(cnxdict)


def create_abnormalities_table(cnxdict):
    """
    Calling this "subkaryotype" rather than clone because not all portions will meet the definition of clone and
    I don't want the confusion of a non-clone clone.
    :param cnxdict:
    :return:
    """
    cnxdict['sql'] = """

        DROP TABLE IF EXISTS temp.abnormality_list ;

        CREATE TABLE `temp`.`abnormality_list` (
          `abnormalityid` int(11) NOT NULL AUTO_INCREMENT,
          `subkaryotypeid_list` varchar({0}) NULL DEFAULT NULL,
          `abnormality_list` varchar({1}) NULL DEFAULT NULL,
          `abnormality_count` int(5) NULL DEFAULT NULL,
          `abnormality_frequency` int(5) NULL DEFAULT NULL,
            PRIMARY KEY (`abnormalityid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

        INSERT INTO temp.abnormality_list (subkaryotypeid_list, abnormality_list, abnormality_count, abnormality_frequency)
            SELECT GROUP_CONCAT(subkaryotypeid) as subkaryotypeid_list
                , abnormality_list
                , abnormality_count
                , COUNT(*) AS abnormality_frequency
            FROM temp.subkaryo_variation
            WHERE abnormality_list <> ''
            GROUP BY abnormality_list, abnormality_count
            ORDER BY COUNT(*) DESC;

        UPDATE temp.subkaryo_variation a, temp.abnormality_list b
            SET a.abnormalityid = b.abnormalityid
            WHERE a.abnormality_list = b.abnormality_list ;
    """.format('1000','500',chr(39))
    dosqlexecute(cnxdict)


def create_unique_subkaryotypes(cnxdict):
    cmd = "SELECT * FROM temp.karyo_variation;"
    df = dosqlread(cmd, cnxdict['cnx'])

    fieldlist = list(df.columns)
    valuelist = df._values[0:df._values.shape[0]]

    for row in valuelist:
        currowdict = getrowdictionary(row, fieldlist)
        curkaryo   = currowdict['karyotype']
        curkaryoid = currowdict['karyotypeid']
        failure_string = parse_subkaryotypes(curkaryoid, curkaryo, cnxdict)
        if failure_string <> '':
            cnxdict['db'].close()
            cnxdict['cnx'].close()
            cnxdict = connect_to_mysql_db_prod('mrcrisk')
            cnxdict['sql'] = """
                INSERT INTO `temp`.`failure` (
                             karyotypeid
                             , karyotype
                             , subkaryotype
                    ) VALUES ( {0}
                              , "{1}"
                              , "{2}"
                        )
            """.format(curkaryoid, curkaryo, failure_string)

            if dosqlexecute(cnxdict, True) != 1:  # reset connection
                try:    cnxdict['cnx'].commit()
                except: pass
                try:    cnxdict['db'].commit()
                except: pass
                cnxdict['db'].close()
                cnxdict['cnx'].close()
                cnxdict = connect_to_mysql_db_prod('mrcrisk')


def parse_subkaryotypes(source_karyo_id,source_karyo, cnxdict):
    p = re.compile(r'\s*')
    clean_source_karyo = re.sub(p, '', source_karyo)  # remove all spaces
    insertsuccess = 'Failed'
    if len(clean_source_karyo.split(']')[:-1]) == 0:
        return insertsuccess

    for subkaryo_loop in source_karyo.split(']')[:-1]:
        if '\<' in subkaryo_loop or '\>' in subkaryo_loop:
            return subkaryo_loop
        if len(subkaryo_loop.lstrip('/').lstrip(' ').split('[')) > 2:
            return subkaryo_loop

        # find subkaryo and chimerism
        splitkaryo = subkaryo_loop.split('[')
        subkaryo = '['.join(splitkaryo[:-1]).lstrip('/').lstrip()
        subkaryo_right = splitkaryo[-1]  # '[{}'.format(splitkaryo[-1])
        stripped_subkaryo = subkaryo.lstrip('/').lstrip()
        subkaryo_full = '{}[{}]'.format(stripped_subkaryo, subkaryo_right).replace('"','')
        chimerism = splitkaryo[0][0:2] == '//'

        print('*'*50)
        print('original karyotype:\n    {}'.format(source_karyo))
        print('subkaryo: {}'.format(subkaryo_full))
        print('chimerism: {}'.format(chimerism))

        # find chromosome and diploidity
        try:
            chromosomes = subkaryo.split('X')[0].strip(',').strip(' ').strip('.')
            p = re.compile(r'\s')
            chromosomes = re.sub(p, ',', chromosomes)

            if ',' in chromosomes:
                chromosomes = chromosomes.split(',')[0].split('<')[0].strip(',').strip(' ')
            if '.' in chromosomes:
                chromosomes = chromosomes.split('.')[0].split('<')[0].strip(',').strip(' ')

            chromosomerange = re.findall(r'\d*', chromosomes)

            if 'R' in chromosomes:
                pass

            for index, item in reversed(list(enumerate(chromosomerange))):
                if item == '':
                    del chromosomerange[index]
            try:
                chromosomerange = map(int, chromosomerange)
                chromosome_min = min(chromosomerange)
                chromosome_max = max(chromosomerange)
                if chromosome_min == chromosome_max:
                    chromosomes = '{}'.format(chromosome_min)
                else:
                    chromosomes = '{}~{}'.format(chromosome_min, chromosome_max)
            except:
                pass

            if chromosome_min == '':
                diploid = 'Undetermined'
            elif chromosome_min >  46:
                diploid = 'Hyperdiploid'
            elif chromosome_min == 46 and chromosome_max == 46 :
                diploid = 'Pseudodiploid'
            elif chromosome_min == 46 and chromosome_max >  46:
                diploid = 'Pseudodiploid/Hyperdiploid'
            elif chromosome_min <  46 and chromosome_max == 46:
                diploid = 'Hyperdiploid/Pseudodiploid'
            elif chromosome_min <  46 and chromosome_max >  46:
                diploid = 'Undetermined'
            elif chromosome_max <  46 :
                diploid = 'Hypodiploid'
            else:
                diploid = 'Undetermined'
        except:
            print('Unable to determine chromosome count and diploidity: \n {}'.format(subkaryo_full))
            return subkaryo_full



        if '~' in subkaryo_full:
            pass

        print('chromosomes and range: {} and {}'.format(chromosomes,chromosomerange))
        print('diploid: {}'.format(diploid))

        # seperate gender and abnormalities
        """
            note that the re pattern used will not find gender if
            entered in lower case because when I did include lower
            case I was picking up x2 as a gender erroneously
            This is the pattern I rejected:
                p = re.compile(r'(X|Y|,-X|,-Y|,\+X|,\+Y)+(,|\[|\\|\;|<|^\d|)?', flags=re.IGNORECASE)  # regular expression pattern
        """
        p = re.compile(r'\d+(.|\s)?(X|Y)+')
        leftsubkaryo = subkaryo.split(',')[0]
        if ',' not in subkaryo:
            try:
                gender = subkaryo.replace(chromosomes, '', 1).strip('.')
                abnormalities = subkaryo.split(gender)[1]
            except:
                gender = 'undefined'
                abnormalities = 'undefined'
                pass
        elif re.search(p, leftsubkaryo) > 0:  # ##XX or ##XY were likely found
            abnormalities = subkaryo.split(',', 1)[1]
            chromosomes = subkaryo.split('X')[0]
            gender = subkaryo.replace(chromosomes, '')
        else:
            abnormalities = subkaryo.split(',', 1)[1]
            try:
                p = re.compile(
                    r'(Xc|Yc|X|Y|,-X|,-Y|,or-X|,or-Y|,\+X|,\+Y|,or\+X|,or\+Y)+(,|\[|\\|\;|<|^\d|)?')  # regular expression pattern
                match = re.search(p, abnormalities).span()
                leftgender = abnormalities[match[0]:match[1]]
                abnormalities = abnormalities.split(leftgender, 1)[1]
                gender = leftgender.strip(',').strip('<').strip(';')
            except:
                abnormalities = subkaryo.replace(chromosomes, '', 1)
                gender = 'undefined'
            abnormalities = '{}'.format(abnormalities.strip(','))
        if abnormalities == 'undefined':
            abnormality_count = None
        elif abnormalities == ' ' or abnormalities == '':
            abnormality_count = 0
        else:
            abnormality_count = len(abnormalities.split(','))
        print('gender: {}'.format(gender))
        print('abnormalities: {}'.format(abnormalities))
        print('abnormality count: {}'.format(abnormality_count))

        # find metaphases and determine if clone
        try:
            metaphases = subkaryo_right
            cells = re.findall(r'\d+', metaphases)[0]
            cells = int(cells)
        except:
            metaphases = ''
            cells = -1

        if ';' in metaphases:
            pass

        print('metaphases: {}'.format(metaphases))
        print('cells: {}'.format(cells))


        # determine if clone_full qualifies as a clone
        if   cells >= 2 and 'Hyper' in diploid:
            is_clone = True
        elif cells >= 2 and 'Pseudo' in diploid:
            is_clone = True
        elif cells >= 3:
            is_clone = True
        else:
            is_clone = False
        print('is_clone: {}'.format(is_clone))


        # determine if clone_full is normal, abnormal, or undefined and
        # count the number of abnormal clones found
        is_normal = cells >= 10 and abnormality_count == 0
        if diploid == 'Hypo' and cells > 2:
            is_abnormal = abnormality_count > 0
        elif diploid != 'Hypo' and cells >= 2:
            is_abnormal = abnormality_count > 0
        else:
            is_abnormal = False

        # is_undefined = cells < 10 and abnormality_count == 0
        if is_clone and is_normal:
            is_undefined = False
        elif is_clone and is_abnormal:
            is_undefined = False
        else:
            is_undefined = True

        print('is_normal: {}'.format(is_normal))
        print('is_abnormal: {}'.format(is_abnormal))
        print('is_undefined: {}'.format(is_undefined))

        cnxdict['sql'] = """
            INSERT INTO `temp`.`subkaryo_variation` (
                           subkaryotype
                         , abnormality_list
                         , abnormality_count
                         , is_chimerism
                         , is_abnormal
                         , diploidity
                         , gender
                         , cells
                         , metaphases
                         , chromosomes
                         , karyotypeid
                         , is_normal
                ) VALUES (  "{0}"
                         ,  "{1}"
                         ,   {2}
                         ,   {3}
                         ,   {4}
                         ,  "{5}"
                         ,  "{6}"
                         ,   {7}
                         ,  "{8}"
                         ,  "{9}"
                         ,  {10}
                         ,  {11}
                    )
        """.format(
                    subkaryo_full            #  0
                   ,abnormalities.strip(' ') #  1
                   ,abnormality_count        #  2
                   ,chimerism                #  3
                   ,is_abnormal              #  4
                   ,diploid                  #  5
                   ,gender                   #  6
                   ,cells                    #  7
                   ,metaphases               #  8
                   ,chromosomes              #  9
                   ,source_karyo_id          # 10
                   ,is_normal)               # 11
        if len(abnormalities)>400:
            pass
        insertsuccess = dosqlexecute(cnxdict,True)
        if insertsuccess!=1: # reset connection
            try: cnxdict['cnx'].commit()
            except: pass
            try: cnxdict['db'].commit()
            except: pass
            return abnormalities.strip(' ')

    return ''


create_subkaryotype_table(cnxdict) # table created will be empty
create_unique_karyotypes(cnxdict)  # table created from allkaryo table in caisis
create_unique_subkaryotypes(cnxdict)
create_abnormalities_table(cnxdict)

"""
Considering compacting the abnormalities and subkaryotypes like this:

CREATE TABLE TEMP.SUBKARYO
    SELECT GROUP_CONCAT(karyotypeid) as karyotypeid_list, a.*
        FROM TEMP.subkaryo_variation a
        GROUP BY subKaryotype;

SELECT GROUP_CONCAT(subkaryotypeid) as subkaryotypeid_list
        , abnormality_list
        , abnormality_count
        , COUNT(*) AS abnormality_frequency
    FROM TEMP.SUBKARYO
    WHERE abnormality_list <> ''
    GROUP BY abnormality_list, abnormality_count
    ORDER BY COUNT(*) DESC;

    select karyotypeid_list, a.*
        from temp.karyo_variation a
        join temp.subkaryo b on LOCATE(concat(',',a.karyotypeid,','),concat(',',b.karyotypeid_list,','))>0;

Note that older copies of this program in the depreciated folder contain logic for calculating the risk from abnormatlity
combined with clonal information and a count of the number of monosomies, trisomies and abnormalities.

"""