import pyodbc 
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=srvmlbsqlt05\cca;"
                      "Database=MPSnapshotProd;"
                      "UID=svcProv;"
                      "PWD=4be7c4de8428d8444f55986c9a68a6cd;"
                      "Trusted_Connection=yes;"
                      "MARS_Connection=Yes;")

# cnxn.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
# cursor = cnxn.cursor()
# cursor.execute('SELECT * FROM Table')

# for row in cursor:
#     print('row = %r' % (row,))