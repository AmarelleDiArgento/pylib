
from pylib.mod.utils import roundBy
# import pymssql as db
import sqlalchemy as db

from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat


def stringConnect(con):
    stringConnect = 'mssql+pymssql://{}:{}@{}/{}'
    return stringConnect.format(
        con['user'],
        con['password'],
        con['server'],
        con['db']
    )


def engineCon(con):
    return db.create_engine(con)


def rowCount(strCon, schema, table):
    try:
        ifexist = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))\n'''.format(
            schema, table)
        rountRows = ifexist + '\tSELECT COUNT(*) rows FROM [{}].[{}];'
        return engineCon(strCon).execute(
            sat(
                rountRows.format(schema, table)
            ).execution_options(stream_results=True)
        ).mappings().all()[0]['rows']

    except:
        return 0


def affectedRows(func):
    def wrapper(*args, **kwargs):
        initialrows = rowCount(
            kwargs['strCon'], kwargs['schema'], kwargs['table'])
        results = func(*args, **kwargs)
        finalrows = rowCount(
            kwargs['strCon'], kwargs['schema'], kwargs['table'])
        affectedRows = finalrows - initialrows
        print('{} to {} affectedRows: {}'.format(
            func.__name__, kwargs['table'], affectedRows))
        return results

    return wrapper


@affectedRows
def truncateTable(strCon, schema, table):
    TruncateTable = 'TRUNCATE TABLE [{}].[{}];'
    engineCon(strCon).execute(
        sat(
            TruncateTable.format(schema, table)
        ).execution_options(autocommit=True)
    )


@affectedRows
def insertDataToSql(strCon, schema, table, data, truncate=False, index=False):
    createTable(strCon, schema, table, data, index)

    if_exists = ('append', 'replace')[truncate]

    data.to_sql(
        schema=schema,
        name=table,
        con=engineCon(strCon),
        if_exists=if_exists,
        index=False
    )


@affectedRows
def deleteDataToSql(strCon, schema, table, where=[]):
    deleteData = ''
    for w in where:
        if deleteData == '':
            deleteData += 'WHERE ' + w
        deleteData += '\n\tAND ' + w

    deleteData = 'DELETE FROM [{}].[{}]'+deleteData + ';'
    print(deleteData)
    engineCon(strCon).execute(
        sat(
            deleteData.format(schema, table)
        ).execution_options(autocommit=True)
    )


def createTableStament(data, schema='dbo', table='newTable', index=False):
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
    if index:
        stament = '\t[ID_{}] int identity(1,1)'.format(table)
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


def createTable(strCon, schema, table, data, index=False):
    TableStament = createTableStament(data, schema, table, index)
    # print(TableStament)
    engineCon(strCon).execute(
        sat(
            TableStament
        ).execution_options(autocommit=True)
    )
