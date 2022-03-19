
from pylib.mod.utils import roundBy
import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat

# dbProjCon = {
#     'Test': {
#         'server': 'elt-dbproj-fca\\testing',
#         'db': 'Logistica',
#         'user': 'talend',
#         'password': 'S3rvT4l3nd*',
#         'tables': {
#             'table': {'schema': '',
#                       'name': ''}
#         }
#     }}


"""
TRANSACCIONES SQL
"""


dbProjCon = ''


def stringConnect(con):

    stringConnect = 'mssql+pymssql://{}:{}@{}/{}'
    return stringConnect.format(
        con['user'],
        con['password'],
        con['server'],
        con['db']
    )


def engineCon(con):
    return sal.create_engine(con)


def insertDataToSql(srtCon, schema, table, data):
    createTable(srtCon, schema, table, data)
    data.to_sql(
        schema=schema,
        name=table,
        con=engineCon(srtCon),
        if_exists='append',
        index=False
    )


def truncateTable(srtCon, schema, table):
    TruncateTable = 'TRUNCATE TABLE {}.{};'
    engineCon(srtCon).execute(
        sat(
            TruncateTable.format(schema, table)
        ).execution_options(autocommit=True)
    )


def createTableStament(data, schema='dbo', table='newTable'):
    columnName = data.columns.values

    datetype = {
        'int64': "int",
        'float64': "float",
        'bool': "tinyint",
        'datetime64[ns]': 'datetime',
        '<M8[ns]': 'NPI',
        'object': 'nvarchar'
    }
    stament = ''
    for c in columnName:

        if stament != '':
            stament += ',\n'
        tipo = datetype[str(data[c].dtype)]
        largo = 0
        if tipo == 'nvarchar':
            largo = data[c].str.len().max()
            largo = roundBy(largo, base=10) + 10
            tipo = '{}({})'.format(tipo, largo)

        stament += '\t[{}] {}'.format(c, tipo)

    return '''
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))
    CREATE TABLE [{}].[{}] (\n{})
    '''.format(schema, table, schema, table, stament)


def createTable(srtCon, schema, table, data):
    TableStament = createTableStament(
        data=data, schema=schema, table=table)
    engineCon(srtCon).execute(
        sat(
            TableStament
        ).execution_options(autocommit=True)
    )
