from Utilities.MySQLdbUtils import *
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')

# cnxdict['sql'] = """
#         DELETE FROM mrcrisk.clone_variations WHERE True;
#     """
# dosqlexecute(cnxdict)

def create_clone_variations():
    cnxdict = connect_to_mysql_db_prod('mrcrisk')
    cnxdict['sql'] = """
    ALTER TABLE `caisis`.`allkaryo`
        ADD COLUMN `cloneid_list` VARCHAR(100) NULL DEFAULT NULL;
    ALTER TABLE `caisis`.`allkaryo`
        ADD FULLTEXT INDEX `pathresult` (`PathTest` ASC);
    UPDATE `caisis`.`allkaryo` SET `cloneid_list` = '';
    """
    dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS `mrcrisk`.`clone_variations`;
    """
    dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        CREATE TABLE `clone_variations` (
          `cloneid` int(11) NOT NULL AUTO_INCREMENT,
          `karyotype` varchar(500) DEFAULT NULL COMMENT 'karyotype',
          `clone` varchar(500) DEFAULT NULL COMMENT 'clone',
          `chromosomes` varchar(15) DEFAULT NULL COMMENT 'chromosomes',
          `diploidity` varchar(30) DEFAULT NULL COMMENT 'diploidity',
          `gender` varchar(15) DEFAULT NULL COMMENT 'diploidity',
          `abnormalities` varchar(500) DEFAULT NULL,
          `abnormality_count` int(11) DEFAULT NULL,
          `metaphases` varchar(6) DEFAULT NULL,
          `cells` int(11) DEFAULT NULL,
          `is_clone` tinyint(4) DEFAULT NULL COMMENT 'meets clonal definition',
          `t(8;21)` tinyint(4) DEFAULT NULL COMMENT 't(8;21)',
          `t(15;17)` tinyint(4) DEFAULT NULL COMMENT 't(15;17)',
          `inv(16)` tinyint(4) DEFAULT NULL COMMENT 'inv(16)',
          `-5 or -7` tinyint(4) DEFAULT NULL COMMENT '-5 or -7',
          `del(5q)` tinyint(4) DEFAULT NULL COMMENT 'del(5q)',
          `del(7q)` tinyint(4) DEFAULT NULL COMMENT 'del(7q)',
          `3q` tinyint(4) DEFAULT NULL COMMENT 'abnormality in 3q',
          `11q` tinyint(4) DEFAULT NULL COMMENT 'abnormality in 11q',
          `17p` tinyint(4) DEFAULT NULL COMMENT 'abnormality in 17p',
          `t(9;11)` tinyint(4) DEFAULT NULL COMMENT 't(9;11)',
          `t(9:22)` tinyint(4) DEFAULT NULL COMMENT 't(9:22)',
          `t(6;9)` tinyint(4) DEFAULT NULL COMMENT 't(6;9)',
          `complex` tinyint(4) DEFAULT NULL COMMENT 'complex karyotype',
          `normal` tinyint(4) DEFAULT NULL COMMENT 'normal karyotype',
          `+8` tinyint(4) DEFAULT NULL COMMENT 'trisomy 8',
          `misc` tinyint(4) DEFAULT NULL COMMENT 'miscellaneousinsufficient',
          `insufficient` tinyint(4) DEFAULT NULL COMMENT 'description',
          `unknown` tinyint(4) DEFAULT NULL COMMENT 'unknown (no data)',
          `marker` tinyint(4) DEFAULT NULL COMMENT 'contains a marker',
          `ring` tinyint(4) DEFAULT NULL COMMENT 'contains a ring',
          `mk` tinyint(4) DEFAULT NULL COMMENT 'monosomal karyotype',
          `mktype` varchar(200) DEFAULT NULL COMMENT 'reason monosomal karyotype',
          `monosomy_count` int(11) DEFAULT NULL COMMENT 'clonal monosomies',
          `monosomy_list` varchar(200) DEFAULT NULL COMMENT 'list of monosomies found',
          `trisomy_count` int(11) DEFAULT NULL COMMENT 'clonal trisomies',
          `trisomy_list` varchar(200) DEFAULT NULL COMMENT 'list of trisomies found',
          PRIMARY KEY (`cloneid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
    """
    dosqlexecute(cnxdict,True)



def getrowdictionary(row, fieldlist):
    rowdict = {}
    for field in fieldlist:
        rowdict[field] = row[fieldlist.index(field)]
    return rowdict


def disect_karyotype(head,source_karyo,source_freq,source_cloneid_list,cnxdict):
    dots = '.' * 20
    p = re.compile(r'\s')
    clean_source_karyo = re.sub(p, '', source_karyo)
    cloneid_list = ''
    if len(clean_source_karyo.split(']')[:-1]) == 0:
        pass
        # print('-' * 20)
        # print ("""The karyotype was incorrectly formatted:\n{}\nRecords like this = {}""".format(source_karyo,source_freq))
        
    clone_count = 0
    abnormal_clone_count = 0

    for loop in source_karyo.split(']')[:-1]:

        karyotype = loop
        if '\<' in karyotype or '\>' in karyotype:
            pass

        if len(karyotype.lstrip('/').lstrip(' ').split('[')) > 2:
            pass

        # a common error in the karyotype is to use a square bracket rather than a paren
        splitkaryo = karyotype.split('[')

        # find clone
        clone = '['.join(splitkaryo[:-1]).lstrip('/').lstrip() # stripped_karyotype.split('[')[0]
        rightkaryotype = splitkaryo[-1] # '[{}'.format(splitkaryo[-1])
        stripped_clone = clone.lstrip('/').lstrip() # karyotype.lstrip('/').lstrip(' ')
        karyotype = '{}[{}]'.format(stripped_clone, rightkaryotype)

        """ removed code
            find out if clone shows chimerism
            if '//' in clone:
                pass
            chimerism = '//' in clone # karyotype.split('[')[0]
            remove leading slashes
            if chimerism:  ## preserve chimersim slashes for display
                karyotype = '//{}]'.format(stripped_clone)
            else:
                karyotype = '{}[{}]'.format(stripped_clone,rightkaryotype)
        """

        # find chromosome and diploidity
        chromosomes = clone.split('X')[0].strip(',').strip(' ').strip('.')
        p = re.compile(r'\s')
        chromosomes = re.sub(p, ',', chromosomes)

        if ',' in chromosomes:
            chromosomes = chromosomes.split(',')[0].split('<')[0].strip(',').strip(' ')
        if '.' in chromosomes:
            chromosomes = chromosomes.split('.')[0].split('<')[0].strip(',').strip(' ')

        chromosomerange = re.findall(r'\d*', chromosomes)
        try:
            if chromosomerange[0] == '':
                diploid = 'Undetermined'
            elif int(chromosomerange[0]) >  46:
                diploid = 'Hyperdiploid'
            elif int(chromosomerange[0]) == 46 and int(chromosomerange[-2]) == 46 :
                diploid = 'Pseudodiploid'
            elif int(chromosomerange[0]) == 46 and int(chromosomerange[-2]) >  46:
                diploid = 'Pseudodiploid/Hyperdiploid'
            elif int(chromosomerange[0]) <  46 and int(chromosomerange[-2]) == 46:
                diploid = 'Hyperdiploid/Pseudodiploid'
            elif int(chromosomerange[0]) <  46 and int(chromosomerange[-2]) >  46:
                diploid = 'Undetermined'
            elif int(chromosomerange[-2]) <  46 :
                diploid = 'Hypodiploid'
            else:
                diploid = 'Undetermined'
        except:
            print(clone)
            print(karyotype)
            pass

        # seperate gender and abnormalities
        """
            note that the re pattern used will not find gender if
            entered in lower case because when I did include lower
            case I was picking up x2 as a gender erroneously
            This is the pattern I rejected:
                p = re.compile(r'(X|Y|,-X|,-Y|,\+X|,\+Y)+(,|\[|\\|\;|<|^\d|)?', flags=re.IGNORECASE)  # regular expression pattern
        """
        p = re.compile(r'\d+(.|\s)?(X|Y)+')
        leftclone = clone.split(',')[0]
        if ',' not in clone:
            gender = clone.replace(chromosomes, '', 1).strip('.')
            abnormalities = clone.split(gender)[1]
        elif re.search(p, leftclone) > 0:  # ##XX or ##XY were likely found
            abnormalities = clone.split(',', 1)[1]
            chromosomes = leftclone.split('X')[0]
            gender = leftclone.replace(chromosomes,'')
        else:
            abnormalities = clone.split(',', 1)[1]
            try:
                p = re.compile(r'(Xc|Yc|X|Y|,-X|,-Y|,or-X|,or-Y|,\+X|,\+Y|,or\+X|,or\+Y)+(,|\[|\\|\;|<|^\d|)?')  # regular expression pattern
                match = re.search(p, abnormalities).span()
                leftgender = abnormalities[match[0]:match[1]]
                abnormalities = abnormalities.split(leftgender, 1)[1]
                gender = leftgender.strip(',').strip('<').strip(';')
            except:
                abnormalities = clone.replace(chromosomes, '', 1)
                gender = ''
                leftgender = ''
            abnormalities = '{} '.format(abnormalities.strip(','))
        if abnormalities == ' ' or abnormalities == '':
            abnormality_count = 0
        else:
            abnormality_count = len(abnormalities.split(','))

        # find metaphases and determine if clone
        try:
            metaphases = rightkaryotype
            cells = re.findall(r'\d+', metaphases)[0]
            cells = int(cells)
        except:
            metaphases = ''
            cells = None

        if len(metaphases)>4:
            pass

        # determine if karyotype qualifies as a clone
        if cells >= 2 and 'Hyper' in diploid:
            is_clone = True
        elif cells>=2 and 'Pseudo' in diploid:
            is_clone = True
        elif cells>=3 and 'Hypo' in diploid:
            is_clone = True
        else:
            is_clone = False

        # test for previous registration
        # registered means added to the table clone_variations
        cmd = """SELECT cloneid FROM `mrcrisk`.`clone_variations` WHERE karyotype = "{}";""".format(karyotype)
        try: cnxdict['cnx'].commit()
        except: pass
        try: cnxdict['db'].commit()
        except: pass
        df = pd.read_sql(cmd,cnxdict['cnx'])
        registered = pd.read_sql(cmd,cnxdict['cnx']).size
        if registered:
            insertid = str(int(df.values[0:1][0])).strip()
            cloneid_list = '{},{}'.format(cloneid_list.strip().strip(','),insertid).strip()
            pass

        if is_clone and not registered:
            clone_count += 1
            abnormal_clone_count += 1
            # only determine risk for clones
            is_t_8_21       = None
            is_t_15_17      = None
            is_inv_16       = None
            is_minus_5_or_7 = None
            is_del5q        = None
            is_del7q        = None
            is_abn3q        = None
            is_t_9_11       = None
            is_abn11q       = None
            is_abn17p       = None
            is_t_9_22       = None
            is_t_6_9        = None
            is_marker       = None
            is_ring         = None
            is_mk           = None
            is_complex      = None
            is_normal       = None
            is_trisomy8     = None
            is_misc         = None
            is_insufficient = None
            is_unknown      = None
            mktype          = ''
            is_monosomy     = {}
            is_trisomy      = {}
            monosomy_count  = 0
            trisomy_count   = 0

            is_insufficient = False
            is_unknown = False
            # t(8:21)
            p = re.compile(r't\(8;21\)')
            is_t_8_21 = re.search(p, abnormalities) != None
            # t(15;17)
            p = re.compile(r't\(15;17\)')
            is_t_15_17 = re.search(p, abnormalities) != None
            # inv(16)
            p = re.compile(r'inv\(16\)')
            is_inv_16 = re.search(p, abnormalities) != None
            # -5 or -7
            p = re.compile(r',?\-(5|7)(,|\[|)')
            is_minus_5_or_7 = re.search(p, abnormalities) != None
            # del 5q
            p = re.compile(r'del\s*\(\s*5\s*\)\s*\(\s*q')
            is_del5q = re.search(p, abnormalities) != None
            # del 7q
            p = re.compile(r'del\s*\(\s*7\s*\)\s*\(\s*q')
            is_del7q = re.search(p, abnormalities) != None
            # abnormality of 3q
            p1 = re.compile(r'(;|der\(|del\(|t\(|add\(|inv\(|-)3[^,].*?q{1}(,|)?')
            p2 = re.compile(r'(3;.*?\d.*?\(q|;3\).*?;q|\(3\)\(q)')
            p3 = re.compile(r'[^0-9.?;\n]3[^0-9.?;\n]{2}q|;3[)]\s{0,1}[(][pq]{1}[0-9.]+;q|[^0-9.?;\n]3;[0-9.]+[)][(]q')
            p3 = re.compile(r'[^0-9.?;\n]3[^0-9.?;\n]{2}q|;3[)]\s{0,1}[(][pq]{1}[0-9.]+;q|\(3;[0-9.;]+[)][(]q|;3;[0-9.]+[)][(][pq]{1}[0-9.]+[;]?q')
            is_abn3q = False
            is_abn3q = re.search(p3, abnormalities) is not None
            if not is_abn3q:
                # p = re.compile(r'[^\d]{1}3[^0-9]+.*?q.*?')
                # p = re.compile(r'(3\s*q|\(\s*3\)\s*\(\s*q|;\s*3\s*\)\s*\(\s*[pq]\s*\d+\.?\d*\s*;\s*q|3\s*;\s*\d+\s*\)\s*\(\s*q)')
                if re.search(p1, abnormalities) != None:
                    for abnormality in abnormalities.strip().split(','):
                        match = re.search(p1, abnormality, )
                        while match is not None:
                            matchspan = re.search(p1, abnormality).span()
                            matchtext = abnormality[matchspan[0]:matchspan[1]]
                            match = re.search(p2, matchtext,)
                            is_abn3q = match is not None
                            if is_abn3q:
                                match = None
                            else:
                                abnormality = abnormality[matchspan[1]:]
            else:
                pass

            # t(9;11)
            p = re.compile(r't\(9;11\)')
            is_t_9_11 = re.search(p, abnormalities) != None
            # abnormality of 11q
            p = re.compile(r'(11\s*q|\(\s*11\)\s*\(\s*q|;\s*11\s*\)\s*\(\s*[pq]\s*\d+\.?\d*\s*;\s*q|11\s*;\s*\d+\s*\)\s*\(\s*q)')
            if re.search(p, abnormalities) != None and not is_t_9_11:
                pass
            is_abn11q = re.search(p, abnormalities) != None and not is_t_9_11
            # abn 17p
            p = re.compile(r'(17\s*p|\(\s*17\)\s*\(\s*p|;\s*17\s*\)\s*\(\s*[pq]\s*\d+\.?\d*\s*;\s*p|17\s*;\s*\d+\s*\)\s*\(\s*p)')
            is_abn17p = re.search(p, abnormalities) != None
            # t(9;22)
            p = re.compile(r't\(9;22\)')
            is_t_9_22 = re.search(p, abnormalities) != None
            # t(6;9)
            p = re.compile(r't\(6;9\)')
            is_t_6_9 = re.search(p, abnormalities) != None
            # marker
            p = re.compile(r'[^a-zA-Z]mar[^a-zA-Z]')
            is_marker = re.search(p, abnormalities) != None
            # ring
            p1 = re.compile(r'[^a-zA-Z]r[^a-zA-Z]')
            p2 = re.compile(r'[^a-zA-Z]ring[^a-zA-Z]')
            is_ring = re.search(p1, abnormalities) != None or re.search(p2, abnormalities) != None
            try: is_trisomy8 = is_trisomy['-8']
            except: is_trisomy8 = False
            # find monosomies
            for i in range(1, 23):
                p = r'-{0}{{1}}[^\da-z]'.format(i)
                p = re.compile(p)
                if re.search(p, abnormalities):
                    is_monosomy['-{}'.format(i)] = True
                    monosomy_count += 1
            # find trisomies
            for i in range(1,23):
                p = r'\+{0}{{1}}[^\da-z]'.format(i)
                p = re.compile(p)
                if re.search(p, abnormalities):
                    is_trisomy['+{}'.format(i)] = True
                    trisomy_count += 1
            # MK
            is_mk = False
            if monosomy_count >= 2:
                mktype = 'a) at least 2 different autosomal (not involving X or Y) monosomies\n' \
                         '   each of which meets criterial for a clone'
                is_mk = True
            if monosomy_count==1 and abnormality_count>1 and trisomy_count==0 and not is_marker and not is_ring:
                mktype = '{}\nb) 1 monosomy and a structural abnormality which is NOT a trisomy or\n' \
                         '   a marker (MAR) or a ring'.format(mktype).strip()
                is_mk = True
            if not is_mk:
                mktype = 'not a monosomal karyotype'
            # normal
            if cells >= 10 and abnormality_count==0:
                is_normal = True
                abnormal_clone_count -= 1
            else:
                is_normal = False
            # complex
            is_complex = abnormality_count >= 3
            if is_complex and abnormality_count == 3 and len(abnormalities.split(','))<3:
                pass
            # miscellaneous
            is_misc = abnormal_clone_count > 1 and not (
                is_t_8_21 or
                is_t_15_17 or
                is_inv_16 or
                is_minus_5_or_7 or
                is_del5q or
                is_del7q or
                is_mk or
                is_abn3q or
                is_abn11q or
                is_abn17p or
                is_t_9_22 or
                is_t_6_9 or
                is_complex or
                is_normal or
                is_trisomy8 or
                is_t_9_11
            )

            # screen output
            if True: # is_normal:
                if head!='':
                    print(head)
                    head = ''
                print ('{} {} {}'.format(dots, 'Karyotype description', dots))  # Favorable
                print ('karyotype: {}'.format(karyotype))
                print ('chromosomes: {}'.format(chromosomes))
                print ('diploidity: {}'.format(diploid))
                print ('gender: {}'.format(gender))
                print ('clone: {}'.format(clone))
                print ('abnormalities: {}'.format(abnormalities))
                print ('abnormality count: {}'.format(abnormality_count))
                print ('metaphases: {} '.format(metaphases))
                print ('cells: {}'.format(cells))
                print ('meets clonal definition: {}'.format(is_clone))

                if is_clone:
                    if is_t_8_21 or is_t_15_17 or is_inv_16:
                        print ('{} {} {}'.format(dots,'Favorable clone',dots)) # Favorable
                        print ('t(8;21): {}'.format(is_t_8_21))
                        print ('t(15;17): {}'.format(is_t_15_17))
                        print ('inv(16): {}'.format(is_inv_16))
                    if is_minus_5_or_7 or is_del5q or is_del7q or is_mk or is_abn3q or is_abn11q or is_abn17p or is_t_9_22 or is_t_6_9:
                        print ('{} {} {}'.format(dots,'Unfavorable clone',dots)) # Favorable
                        print ('-5 or -7: {}'.format(is_minus_5_or_7))
                        print ('del(5q): {}'.format(is_del5q))
                        print ('del(7q): {}'.format(is_del7q))
                        print ('monosomal karyotype: {}'.format(is_mk))
                        print ('abnormality in 3q: {}'.format(is_abn3q))
                        print ('abnormality in 11q: {}'.format(is_abn11q))
                        print ('abnormality in 17p: {}'.format(is_abn17p))
                        print ('t(9;22): {}'.format(is_t_9_22))
                        print ('t(6;9): {}'.format(is_t_6_9))
                    if is_normal or is_trisomy8 or is_t_9_11 or is_misc:
                        print ('complex karyotype: {}'.format(is_complex))
                        print ('{} {} {}'.format(dots,'Intermediate clone',dots)) # Favorable
                        print ('normal: {}'.format(is_normal))
                        print ('trisomy 8: {}'.format(is_trisomy8))
                        print ('t(9;11): {}'.format(is_t_9_11))
                        print ('miscellaneous: {}'.format(is_misc))
                    if is_insufficient or is_unknown:
                        print ('{} {} {}'.format(dots,'Clone Not Categorized',dots)) # Favorable
                        print ('insufficient: {}'.format(is_insufficient))
                        print ('unknown (no data): {}'.format(is_unknown))

                    if is_trisomy8 or is_mk:
                        print ('{} {} {}'.format(dots, 'Details', dots))  # Favorable

                    if is_mk:
                        print ('clonal monosomies: {}'.format(monosomy_count))
                        print ('monosomy: {}'.format(is_monosomy))
                        print ('marker: {}'.format(is_marker))
                        print ('ring: {}'.format(is_ring))

                    if is_trisomy8 or is_mk:
                        print ('clonal trisomies: {}'.format(trisomy_count))
                        print ('trisomy: {}'.format(is_trisomy))

            print('*'*20)
            print ('abnormal clones found: {}'.format(abnormal_clone_count))
            print ('total clones found: {}'.format(clone_count))

            # register clone
            """

            """
            cnxdict['sql'] = """
                INSERT INTO mrcrisk.clone_variations
                    SET `karyotype` = "{}"
                    , `chromosomes` = "{}"
                    , `diploidity` = "{}"
                    , `gender` = "{}"
                    , `clone` = "{}"
                    , `abnormalities` = "{}"
                    , `abnormality_count` = {}
                    , `metaphases` = "{}"
                    , `cells` = {}
                    , is_clone = {}
                    , `t(8;21)` = {}
                    , `t(15;17)` = {}
                    , `inv(16)` = {}
                    , `-5 or -7` = {}
                    , `del(5q)` = {}
                    , `del(7q)` = {}
                    , `3q` = {}
                    , `11q` = {}
                    , `17p` = {}
                    , `t(9;11)` = {}
                    , `t(9:22)` = {}
                    , `t(6;9)` = {}
                    , `complex` = {}
                    , `normal` = {}
                    , `+8` = {}
                    , `misc` = {}
                    , `insufficient` = {}
                    , `unknown` = {}
                    , `marker` = {}
                    , `ring` = {}
                    , `mk` = {}
                    , `mktype` = "{}"
                    , `monosomy_count` = {}
                    , `monosomy_list` = "{}"
                    , `trisomy_count` = {}
                    , `trisomy_list` = "{}"
            """.format(karyotype
                    ,chromosomes
                    ,diploid
                    ,gender
                    ,clone
                    ,abnormalities
                    ,abnormality_count
                    ,metaphases
                    ,cells
                    ,is_clone
                    ,is_t_8_21
                    ,is_t_15_17
                    ,is_inv_16
                    ,is_minus_5_or_7
                    ,is_del5q
                    ,is_del7q
                    ,is_abn3q
                    ,is_abn11q
                    ,is_abn17p
                    ,is_t_9_11
                    ,is_t_9_22
                    ,is_t_6_9
                    ,is_complex
                    ,is_normal
                    ,is_trisomy8
                    , is_misc
                    , is_insufficient
                    , is_unknown
                    , is_marker
                    , is_ring
                    , is_mk
                    , mktype.strip()
                    , monosomy_count
                    , str(is_monosomy).strip('{').strip('}')
                    , trisomy_count
                    , str(is_trisomy).strip('{').strip('}')
                    )
            insertsuccess = dosqlexecute(cnxdict,True)
            if insertsuccess<0: # stop early
                return insertsuccess
            else:
                # test for previous registration
                # registered means added to the table clone_variations
                cmd = """SELECT cloneid FROM `mrcrisk`.`clone_variations` WHERE karyotype = "{}";""".format(karyotype)
                try: cnxdict['cnx'].commit()
                except: pass
                try: cnxdict['db'].commit()
                except: pass
                df = pd.read_sql(cmd, cnxdict['cnx'])
                insertid = str(int(df.values[0:1][0])).strip()
                cloneid_list = '{},{}'.format(cloneid_list.strip(), insertid).strip()

    cnxdict['sql'] = """
        UPDATE `caisis`.`allkaryo` SET cloneid_list = '{}' WHERE pathresult = "{}";
    """.format(cloneid_list.strip().strip(','),source_karyo)
    if ',{},'.format(insertid) not in ',{},'.format(source_cloneid_list):
        dosqlexecute(cnxdict,True)

    return 1


def update_clone_variations():
    cnxdict = connect_to_mysql_db_prod('mrcrisk')
    cmd = """
        SELECT pathresult, cloneid_list, count(*) AS freq
            FROM `caisis`.`allkaryo`
            WHERE lower(pathresult) not rlike '(inevaluable|please see comment|insufficient|n\/a|cancel|failed|unk|inadequate|inconclusive)'
                and lower(pathresult) not rlike '(^not|^no[[:space:]]|nuc[[:space:]])'
                and ltrim(rtrim(pathresult)) <> ''
                and (`type` like '%cyto%' or `Pathtest` like '%cyto%')
            group by ltrim(rtrim(pathresult))
            order by count(*) desc, ltrim(pathresult)
            ;
        """
    df = dosqlread(cmd, cnxdict['cnx'])

    fieldlist = list(df.columns)
    valuelist = df._values[0:df._values.shape[0]]

    for row in valuelist:
        currowdict = getrowdictionary(row, fieldlist)
        curkaryo   = currowdict['pathresult']
        curfreq    = currowdict['freq']
        curclonelist = currowdict['cloneid_list']
        heading    = '{}\nFull cytogenetic information:\n{}'.format('*'*20,curkaryo)
        try:
            success = disect_karyotype(heading,curkaryo,curfreq,curclonelist,cnxdict)
        except:
            success = -1
            pass
        if success < 0:
            print('FAILED TO PARSE KARYOTYPE: {}'.format(curkaryo))
            cnxdict['db'].close()
            cnxdict['cnx'].close()
            cnxdict = connect_to_mysql_db_prod('mrcrisk')


# run this to fill the clone variations table
update_clone_variations()

def create_output():
    cnxdict = connect_to_mysql_db_prod('mrcrisk')
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')
    cmd = """
        select * from mrcrisk.allcytorisk;
    """
    df = pd.read_sql(cmd, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='All Karyo Risk', index=False)

# run this to fill create the clone variations table
# create_clone_variations()
# run this to fill the clone variations table
# update_clone_variations()
create_output()