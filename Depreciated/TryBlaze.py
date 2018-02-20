from Utilities.Connection import *
from Utilities.SQLServerUtils import *
import blaze as blz
from sqlalchemy import create_engine

cnxdict = read_db_config('caisiswork')
cnxdict = connect_to_caisisprod(cnxdict)
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd'],collation='Latin1_General'))
# engine = create_engine(cnxdict)
engine = create_engine('{SQL Server};SERVER=CONGO - H\H;DATABASE=WorkDBProd;TDS_Version=8.0;unicode_results=True;CHARSET=UTF8;TRUSTED_CONNECTION=yes')
db = blz.data(engine)
