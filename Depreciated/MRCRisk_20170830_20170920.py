from Utilities.MySQLdbUtils import *
reload(sys)

"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')

# cnxdict['sql'] = """
#         DELETE FROM mrcrisk.clone_variation WHERE True;
#     """
# dosqlexecute(cnxdict)

def create_variation_tables():
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
        DROP TABLE IF EXISTS `mrcrisk`.`karyo_variation`;
    """
    dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        CREATE TABLE `mrcrisk`.`karyo_variation` (
          `karyoid` int(11) NOT NULL AUTO_INCREMENT,
          `abnormality_count` int(11) DEFAULT NULL,
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
          `misc` tinyint(4) DEFAULT NULL COMMENT 'miscellaneous',
          `insufficient` tinyint(4) DEFAULT NULL COMMENT 'insufficient',
          `unknown` tinyint(4) DEFAULT NULL COMMENT 'unknown (no data)',
          `marker` tinyint(4) DEFAULT NULL COMMENT 'contains a marker',
          `ring` tinyint(4) DEFAULT NULL COMMENT 'contains a ring',
          `mk` tinyint(4) DEFAULT NULL COMMENT 'monosomal karyotype',
          `mktype` varchar(200) DEFAULT NULL COMMENT 'reason monosomal karyotype',
          `monosomy_count` int(11) DEFAULT NULL COMMENT 'clonal monosomies',
          `monosomy_list` varchar(200) DEFAULT NULL COMMENT 'list of monosomies found',
          `trisomy_count` int(11) DEFAULT NULL COMMENT 'clonal trisomies',
          `trisomy_list` varchar(200) DEFAULT NULL COMMENT 'list of trisomies found',
          PRIMARY KEY (`karyoid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
    """
    dosqlexecute(cnxdict,True)

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS `mrcrisk`.`clone_variation`;
    """
    dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        CREATE TABLE `mrcrisk`.`clone_variation` (
          `cloneid` int(11) NOT NULL AUTO_INCREMENT,
          `clone` varchar(500) DEFAULT NULL COMMENT 'clone',
          `chromosomes` varchar(15) DEFAULT NULL COMMENT 'chromosomes',
          `diploidity` varchar(30) DEFAULT NULL COMMENT 'diploidity',
          `gender` varchar(15) DEFAULT NULL COMMENT 'diploidity',
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


    # Not sure this table will get used
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS `mrcrisk`.`abnormality_variation`;
    """
    dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        CREATE TABLE `mrcrisk`.`abnormality_variation` (
          `abnormid` int(11) NOT NULL AUTO_INCREMENT,
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
          PRIMARY KEY (`abnormid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
    """
    dosqlexecute(cnxdict, True)


def abnormality_check(resultdict,abnormalitylist):
    p = re.compile(resultdict['pattern'])
    resultdict['result'] = re.search(p, abnormalitylist) is not None
    if resultdict['result']:
        matchspan = re.search(p, abnormalitylist).span()
        if not '<' + resultdict['abnormalitytest'] + '>' in resultdict['matchtext']:
            resultdict['matchtext'] = \
                resultdict['matchtext'] + \
                '<' + resultdict['abnormalitytest'] + '>' + \
                abnormalitylist[matchspan[0]:matchspan[1]] + \
                '</' + resultdict['abnormalitytest'] + '>\n'


def test_t_8_21(resultdict,abnormalitylist): #$'t\(15;17\)'
    resultdict['pattern'] = r't\(8;21\)'
    resultdict['abnormalitytest'] = 't(8;21)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_t_15_17(resultdict,abnormalitylist):
    resultdict['pattern'] = r't\(15;17\)'
    resultdict['abnormalitytest'] = 't(15;17)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_inv_16(resultdict,abnormalitylist):
    resultdict['pattern'] = r'inv\(16\)'
    resultdict['abnormalitytest'] = 'inv(16)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_minus_5_or_7(resultdict, abnormalitylist):
    resultdict['pattern'] = r',?\-(5|7)(,|\[|)'
    resultdict['abnormalitytest'] = '-5 or -7'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_del5q(resultdict, abnormalitylist):
    resultdict['pattern'] = r'del\s*\(\s*5\s*\)\s*\(\s*q'
    resultdict['abnormalitytest'] = 'del(5q)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_del7q(resultdict, abnormalitylist):
    resultdict['pattern'] = r'del\s*\(\s*7\s*\)\s*\(\s*q'
    resultdict['abnormalitytest'] = 'del(7q)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_9_11(resultdict, abnormalitylist):
    resultdict['pattern'] = r't\(9;11\)'
    resultdict['abnormalitytest'] = 't(9;11)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_abn3q(resultdict, abnormalitylist):
    resultdict['pattern'] = r'[(]3[)][(]q|' \
                            r';3[)]\s*[(][pq?]{1}[0-9.]*;q|' \
                            r'\(3;[0-9.;]*[)]\s*[(]q|' \
                            r';3;[0-9.?]*[)]\s*[(][pq?\s0-9.]*[;]?'
    resultdict['abnormalitytest'] = '3q'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_abn11q(resultdict, abnormalitylist):
    resultdict['pattern'] = r'[(]11[)][(]q|' \
                            r';11[)]\s*[(][pq?]{1}[0-9.]*;q|' \
                            r'\(11;[0-9.;]*[)]\s*[(]q|' \
                            r';11;[0-9.?]*[)]\s*[(][pq?\s0-9.]*[;]?q'
    resultdict['abnormalitytest'] = '11q'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result'] and not test_9_11(resultdict, abnormalitylist)


def test_abn17p(resultdict, abnormalitylist):
    resultdict['pattern'] = r'[(]17[)][(]p|' \
                            r';17[)]\s*[(][pq?]{1}[0-9.]*;p|' \
                            r'\(17;[0-9.;]*[)]\s*[(]p|' \
                            r';17;[0-9.?]*[)]\s*[(][pq?\s0-9.]*[;]?p'
    resultdict['abnormalitytest'] = '17p'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_9_22(resultdict, abnormalitylist):
    resultdict['pattern'] = r't\(9;22\)'
    resultdict['abnormalitytest'] = 't(9;22)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_6_9(resultdict, abnormalitylist):
    resultdict['pattern'] = r't\(6;9\)'
    resultdict['abnormalitytest'] = 't(6;9)'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_marker(resultdict, abnormalitylist):
    resultdict['pattern'] = r'[^a-zA-Z]mar[^a-zA-Z]'
    resultdict['abnormalitytest'] = 'marker'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_ring(resultdict, abnormalitylist):
    resultdict['pattern'] = r'[^a-zA-Z](r|ring)[^a-zA-Z]'
    resultdict['abnormalitytest'] = 'ring'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']


def test_trisomy8(resultdict, abnormalitylist):
    resultdict['pattern'] = r'\+8[^0-9]'
    resultdict['abnormalitytest'] = '+8'
    abnormality_check(resultdict, abnormalitylist)
    return resultdict['result']

# try: is_trisomy8 = is_trisomy['-8']
# except: is_trisomy8 = False

def test_monosomy(resultdict, abnormalitylist):
    monosomy_count = 0
    for i in range(1, 24):
        resultdict['pattern'] = r'-\s*{0}\s*(,|\[)'.format(i)
        resultdict['abnormalitytest'] = '-{}'.format(i)
        abnormality_check(resultdict, abnormalitylist)
        monosomy_count += resultdict['result']
    return monosomy_count


def test_trisomy(resultdict, abnormalitylist):
    trisomy_count = 0
    for i in range(1, 24):
        resultdict['pattern'] = r'\+\s*{0}\s*(,|\[)'.format(i)
        resultdict['abnormalitytest'] = '+{}'.format(i)
        abnormality_check(resultdict, abnormalitylist)
        trisomy_count += resultdict['result']
    return trisomy_count


# find monosomies



def disect_karyotype(head,source_karyo,source_freq,source_cloneid_list,cnxdict):
    dots = '.' * 20
    equals = '=' * 20
    p = re.compile(r'\s')
    clean_source_karyo = re.sub(p, '', source_karyo) # remove all spaces
    cloneid_list = ''
    insertsuccess = -1
    if len(clean_source_karyo.split(']')[:-1]) == 0:
        return insertsuccess
        # print('-' * 20)
        # print ("""
        #     The karyotype was incorrectly formatted:\n{}\nRecords like this = {}
        # """.format(source_karyo,source_freq))
        
    clone_count = 0
    abnormal_clone_count = 0
    monosomy_count = 0
    resultdict = {
        'result': False,
        'matchtext': '',
        'pattern': '',
        'abnormalitytest': ''
    }

    print('\n{} {} {}'.format(equals,'Start of disection',equals))
    print('source karyotype: {}'.format(clean_source_karyo))

    for clone_loop in source_karyo.split(']')[:-1]:

        if '\<' in clone_loop or '\>' in clone_loop:
            pass

        if len(clone_loop.lstrip('/').lstrip(' ').split('[')) > 2:
            pass

        # a common error in the karyotype is to use a square bracket rather than a paren
        splitkaryo = clone_loop.split('[')

        # find clone
        clone = '['.join(splitkaryo[:-1]).lstrip('/').lstrip()
        clone_right = splitkaryo[-1] # '[{}'.format(splitkaryo[-1])
        stripped_clone = clone.lstrip('/').lstrip()
        clone_full = '{}[{}]'.format(stripped_clone, clone_right)

        """ removed code
            find out if clone shows chimerism
            if '//' in clone:
                pass
            chimerism = '//' in clone # karyotype.split('[')[0]
            remove leading slashes
            if chimerism:  ## preserve chimersim slashes for display
                karyotype = '//{}]'.format(stripped_clone)
            else:
                karyotype = '{}[{}]'.format(stripped_clone,clone_right)
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
            print(clone_full)
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
            metaphases = clone_right
            cells = re.findall(r'\d+', metaphases)[0]
            cells = int(cells)
        except:
            metaphases = ''
            cells = None

        if len(metaphases)>4:
            pass

        # determine if clone_full qualifies as a clone
        if cells >= 2 and 'Hyper' in diploid:
            is_clone = True
        elif cells>=2 and 'Pseudo' in diploid:
            is_clone = True
        elif cells>=3 and 'Hypo' in diploid:
            is_clone = True
        else:
            is_clone = False

        # determine if clone_full is normal, abnormal, or undefined and
        # count the number of abnormal clones found
        is_normal = cells >= 10 and abnormality_count == 0
        is_undefined = cells < 10 and abnormality_count == 0
        if diploid == 'Hypo' and cells > 2:
            is_abnormal = abnormality_count > 0
        elif diploid != 'Hypo' and cells >= 2:
            is_abnormal = abnormality_count > 0
        else:
            is_abnormal = 0

        abnormal_clone_count += is_abnormal and is_clone


        # test for previous registration
        # registered means added to the table clone_variation
        cmd = """SELECT cloneid FROM `mrcrisk`.`clone_variation` WHERE clone = "{}";""".format(clone_full)
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


        if not is_clone:
            print ('{} {} {}'.format(dots, 'Non-clonal description', dots))
            print('Not clonal: {}'.format(clone_full))

        if is_clone and not registered:
            clone_count += 1
            # # only determine risk for clones
            # is_mk           = None
            # is_complex      = None
            # is_normal       = None
            # is_trisomy8     = None
            # is_misc         = None
            # is_insufficient = None
            # is_unknown      = None
            # mktype          = ''
            # is_monosomy     = {}
            # is_trisomy      = {}
            # trisomy_count   = 0

            is_insufficient = False
            is_unknown = False

            is_t_8_21       = test_t_8_21(resultdict, abnormalities)
            is_t_15_17      = test_t_15_17(resultdict, abnormalities)
            is_inv_16       = test_inv_16(resultdict, abnormalities)
            is_minus_5_or_7 = test_minus_5_or_7(resultdict, abnormalities)
            is_del5q        = test_del5q(resultdict, abnormalities)
            is_del7q        = test_del7q(resultdict, abnormalities)
            is_t_9_11       = test_9_11(resultdict, abnormalities)
            is_abn3q        = test_abn3q(resultdict, abnormalities)
            is_abn11q       = test_abn3q(resultdict, abnormalities)
            is_abn17p       = test_abn3q(resultdict, abnormalities)
            is_t_9_22       = test_9_22(resultdict, abnormalities)
            is_t_6_9        = test_6_9(resultdict, abnormalities)
            is_marker       = test_marker(resultdict, abnormalities)
            is_ring         = test_ring(resultdict, abnormalities)
            is_trisomy8     = test_trisomy8(resultdict, abnormalities)
            monosomy_count  = test_monosomy(resultdict, abnormalities)
            print(monosomy_count)
            trisomy_count   = test_trisomy(resultdict, abnormalities)
            print(trisomy_count)
            # # find trisomies
            # for i in range(1,23):
            #     p = r'\+{0}{{1}}[^\da-z]'.format(i)
            #     p = re.compile(p)
            #     if re.search(p, abnormalities):
            #         is_trisomy['+{}'.format(i)] = True
            #         trisomy_count += 1
            # # MK
            # is_mk = False
            # if monosomy_count >= 2:
            #     mktype = 'a) at least 2 different autosomal (not involving X or Y) monosomies\n' \
            #              '   each of which meets criterial for a clone'
            #     is_mk = True
            # if monosomy_count==1 and abnormality_count>1 and trisomy_count==0 and not is_marker and not is_ring:
            #     mktype = '{}\nb) 1 monosomy and a structural abnormality which is NOT a trisomy or\n' \
            #              '   a marker (MAR) or a ring'.format(mktype).strip()
            #     is_mk = True
            # if not is_mk:
            #     mktype = 'not a monosomal karyotype'
            # # complex
            # is_complex = abnormality_count >= 3
            # if is_complex and abnormality_count == 3 and len(abnormalities.split(','))<3:
            #     pass
            # # miscellaneous
            # is_misc = abnormal_clone_count > 1 and not (
            #     is_t_8_21 or
            #     is_t_15_17 or
            #     is_inv_16 or
            #     is_minus_5_or_7 or
            #     is_del5q or
            #     is_del7q or
            #     is_mk or
            #     is_abn3q or
            #     is_abn11q or
            #     is_abn17p or
            #     is_t_9_22 or
            #     is_t_6_9 or
            #     is_complex or
            #     is_normal or
            #     is_trisomy8 or
            #     is_t_9_11
            # )

            # screen output
            if True: # is_normal:
                # if head!='':
                #     print(head)
                #     head = ''
                print ('{} {} {}'.format(dots, 'Clonal abnormality description', dots))  # Favorable
                print ('source karyotype: {}'.format(source_karyo))
                print ('characteristics of abnormal clone {}: {}'.format(clone_count,clone_full))
                print ('chromosomes: {}'.format(chromosomes))
                print ('diploidity: {}'.format(diploid))
                print ('gender: {}'.format(gender))
                print ('clone abnormality list: {}'.format(abnormalities.split(',')))
                print ('abnormality count: {}'.format(abnormality_count))
                print ('metaphases: {} '.format(metaphases))
                print ('cells: {}'.format(cells))
                print ('meets clonal definition: {}'.format(is_clone))

                if is_clone:
                    print ('    normal: {}'.format(is_normal))
                    print ('    abnormal: {}'.format(is_abnormal))
                    print ('    normality not defined : {}'.format(is_undefined))
                    if is_t_8_21 or is_t_15_17 or is_inv_16:
                        print ('    {} {} {}'.format(dots,'Favorable clonal abnormality',dots)) # Favorable
                        print ('    t(8;21): {}'.format(is_t_8_21))
                        print ('    t(15;17): {}'.format(is_t_15_17))
                        print ('    inv(16): {}'.format(is_inv_16))
                    if is_minus_5_or_7 or is_del5q or is_del7q or is_abn3q or is_abn11q or is_abn17p or is_t_9_22 or is_t_6_9: # or is_mk
                        print ('    {} {} {}'.format(dots,'Unfavorable clonal abnormality',dots)) # Favorable
                        print ('    -5 or -7: {}'.format(is_minus_5_or_7))
                        print ('    del(5q): {}'.format(is_del5q))
                        print ('    del(7q): {}'.format(is_del7q))
                        # print ('monosomal karyotype: {}'.format(is_mk))
                        print ('    abnormality in 3q: {}'.format(is_abn3q))
                        print ('    abnormality in 11q: {}'.format(is_abn11q))
                        print ('    abnormality in 17p: {}'.format(is_abn17p))
                        print ('    t(9;22): {}'.format(is_t_9_22))
                        print ('    t(6;9): {}'.format(is_t_6_9))
                    if is_normal or is_trisomy8 or is_t_9_11:
                    # if is_normal or is_trisomy8 or is_t_9_11 or is_misc:
                        # print ('complex karyotype: {}'.format(is_complex))
                        print ('    {} {} {}'.format(dots,'Intermediate clonal abnormality',dots)) # Favorable
                        print ('    normal: {}'.format(is_normal))
                        print ('    trisomy 8: {}'.format(is_trisomy8))
                        print ('    t(9;11): {}'.format(is_t_9_11))
                        # print ('miscellaneous: {}'.format(is_misc))
                    if is_insufficient or is_unknown:
                        print ('    {} {} {}'.format(dots,'Clonal abnormality not categorized',dots)) # Favorable
                        print ('    insufficient: {}'.format(is_insufficient))
                        print ('    unknown (no data): {}'.format(is_unknown))
                #
                #     if is_trisomy8 or is_mk:
                #         print ('    {} {} {}'.format(dots, 'Details', dots))  # Favorable
                #
                #     if is_mk:
                #         print ('    clonal monosomies: {}'.format(monosomy_count))
                #         print ('    monosomy: {}'.format(is_monosomy))
                    print ('    marker: {}'.format(is_marker))
                    print ('    ring: {}'.format(is_ring))
                #
                #     if is_trisomy8 or is_mk:
                #         print ('clonal trisomies: {}'.format(trisomy_count))
                #         print ('trisomy: {}'.format(is_trisomy))

    print ('{} {} {}'.format(dots, 'Summary of clonal abnormalities found', dots))  # Favorable
    print ('source karyotype: {}'.format(source_karyo))
    print ('abnormal clones found: {}'.format(abnormal_clone_count))
    print ('total clones found: {}'.format(clone_count))


            # register clone
            # cnxdict['sql'] = """
            #     INSERT INTO mrcrisk.clone_variation
            #         SET `karyotype` = "{}"
            #         , `chromosomes` = "{}"
            #         , `diploidity` = "{}"
            #         , `gender` = "{}"
            #         , `clone` = "{}"
            #         , `abnormalities` = "{}"
            #         , `abnormality_count` = {}
            #         , `metaphases` = "{}"
            #         , `cells` = {}
            #         , is_clone = {}
            #         , `t(8;21)` = {}
            #         , `t(15;17)` = {}
            #         , `inv(16)` = {}
            #         , `-5 or -7` = {}
            #         , `del(5q)` = {}
            #         , `del(7q)` = {}
            #         , `3q` = {}
            #         , `11q` = {}
            #         , `17p` = {}
            #         , `t(9;11)` = {}
            #         , `t(9:22)` = {}
            #         , `t(6;9)` = {}
            #         , `complex` = {}
            #         , `normal` = {}
            #         , `+8` = {}
            #         , `misc` = {}
            #         , `insufficient` = {}
            #         , `unknown` = {}
            #         , `marker` = {}
            #         , `ring` = {}
            #         , `mk` = {}
            #         , `mktype` = "{}"
            #         , `monosomy_count` = {}
            #         , `monosomy_list` = "{}"
            #         , `trisomy_count` = {}
            #         , `trisomy_list` = "{}"
            # """.format(karyotype
            #         ,chromosomes
            #         ,diploid
            #         ,gender
            #         ,clone
            #         ,abnormalities
            #         ,abnormality_count
            #         ,metaphases
            #         ,cells
            #         ,is_clone
            #         ,is_t_8_21
            #         ,is_t_15_17
            #         ,is_inv_16
            #         ,is_minus_5_or_7
            #         ,is_del5q
            #         ,is_del7q
            #         ,is_abn3q
            #         ,is_abn11q
            #         ,is_abn17p
            #         ,is_t_9_11
            #         ,is_t_9_22
            #         ,is_t_6_9
            #         ,is_complex
            #         ,is_normal
            #         ,is_trisomy8
            #         , is_misc
            #         , is_insufficient
            #         , is_unknown
            #         , is_marker
            #         , is_ring
            #         , is_mk
            #         , mktype.strip()
            #         , monosomy_count
            #         , str(is_monosomy).strip('{').strip('}')
            #         , trisomy_count
            #         , str(is_trisomy).strip('{').strip('}')
            #         )
            # insertsuccess = dosqlexecute(cnxdict,True)
            # if insertsuccess<0: # stop early
            #     return insertsuccess
            # else:
            #     # test for previous registration
            #     # registered means added to the table clone_variation
            #     cmd = """SELECT cloneid FROM `mrcrisk`.`clone_variation` WHERE karyotype = "{}";""".format(karyotype)
            #     try: cnxdict['cnx'].commit()
            #     except: pass
            #     try: cnxdict['db'].commit()
            #     except: pass
            #     df = pd.read_sql(cmd, cnxdict['cnx'])
            #     insertid = str(int(df.values[0:1][0])).strip()
            #     cloneid_list = '{},{}'.format(cloneid_list.strip(), insertid).strip()

    # cnxdict['sql'] = """
    #     UPDATE `caisis`.`allkaryo` SET cloneid_list = '{}' WHERE pathresult = "{}";
    # """.format(cloneid_list.strip().strip(','),source_karyo)
    # if ',{},'.format(insertid) not in ',{},'.format(source_cloneid_list):
    #     dosqlexecute(cnxdict,True)

    return 1


def update_clone_variation():
    cnxdict = connect_to_mysql_db_prod('mrcrisk')
    cnxdict['sql'] = """
        SET @idx := 0;
        DROP TABLE IF EXISTS temp.karyo_variation ;
        CREATE TABLE temp.karyo_variation
        SELECT @idx:=@idx + 1 AS recnum
            , pathresult
            , cloneid_list
            , count(*) AS freq
            FROM `caisis`.`allkaryo`
            WHERE LOWER(pathresult) NOT RLIKE '(inevaluable|please see comment|insufficient|n\/a|cancel|failed|unk|inadequate|inconclusive)'
                and LOWER(pathresult) NOT RLIKE '(^not|^no[[:space:]]|nuc[[:space:]])'
                and LTRIM(RTRIM(pathresult)) <> ''
                and (`type` LIKE '%cyto%' OR `Pathtest` LIKE '%cyto%')
                and pathresult LIKE '%]/4%'
                and (pathresult LIKE '%r[%' OR pathresult LIKE '%ring%')
            GROUP BY LTRIM(RTRIM(pathresult))
            ORDER BY COUNT(*) DESC, LTRIM(pathresult)
            LIMIT 20
    """
    dosqlexecute(cnxdict)
    cmd = "SELECT * FROM temp.karyo_variation;"
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
        if success < 0:
            print('FAILED TO PARSE KARYOTYPE: {}'.format(curkaryo))
            cnxdict['db'].close()
            cnxdict['cnx'].close()
            cnxdict = connect_to_mysql_db_prod('mrcrisk')



def create_output():
    cnxdict = connect_to_mysql_db_prod('mrcrisk')
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')
    cmd = """
        select * from mrcrisk.allcytorisk;
    """
    df = pd.read_sql(cmd, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='All Karyo Risk', index=False)

# run this to fill create the clone variations table
create_variation_tables()
# run this to fill the clone variations table
update_clone_variation()
# create_output()