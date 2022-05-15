

from cmath import nan
import numpy as np
from pylib.mod.utils import excutionTime, roundBy
import pymssql as sql
import sqlalchemy as db
from sqlalchemy.orm import Session
from sqlalchemy import create_engine as sen, text as sat
from sqlalchemy.schema import Table,  MetaData, Column
from sqlalchemy.types import Integer, DateTime, String, Float, Boolean

import math

# from sqlalchemy import Profiler

# Profiler.init("bulk_inserts", num=100000)


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


def truncateTable(strCon, schema, table):

    ifexist = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))\n'''.format(
        schema, table)
    TruncateTable = ifexist + 'TRUNCATE TABLE [{}].[{}];'.format(schema, table)
    # print(TruncateTable)
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


def alchemy_col(name, type, leng):
    switch = {
        'int64': Integer,
        'float64': Float,
        'bool': Boolean,
        'datetime64[ns]': DateTime
    }
    # print(name, type, leng,  switch.get(type, String(leng)))
    return Column(name, switch.get(type, String(leng)))


# @affectedRows
# @Profiler.profile
def insertDataToSql_Alchemy(strCon, schema, table, data, truncate=False, depureColumns=[], index=False, n=10000):

    Columns = createTable(strCon, schema, table, data, index)
    try:

        if truncate:
            truncateTable(strCon, schema, table)

        if depureColumns.__len__() > 0:
            depure(strCon, data, schema, table, depureColumns)

        table = Table(
            table,
            MetaData(bind=engineCon(strCon)),
            *Columns,
            schema=schema
        )

        total = data.shape[0]
        cicle = math.floor(total / n)
        residue = total % n
        ini = 0
        end = n

        for _ in range(33, cicle):

            df = data.iloc[ini:end]
            df = df.replace(np.nan, 0)
            dic = df.to_dict(orient='records')

            engineCon(strCon).execute(
                table.insert(), dic
            )
            ini = end
            end = end + n
            print('insert {} to {} rows'.format(end, total))

        df = data.iloc[ini:total]
        df = df.replace(np.nan, 0)
        dic = df.to_dict(orient='records')
        engineCon(strCon).execute(
            table.insert(), dic
        )

        print('insert {} to {} rows'.format(total, total))
    except sql.Error as ex:  # Bad >:[
        raise ex
    except Exception as ex:  # Bad. >:[
        raise ex


# @affectedRows
def deleteDataToSql(strCon, schema, table, where=[]):
    deleteData = ''
    for w in where:
        if deleteData == '':
            deleteData += 'WHERE ' + w
        deleteData += '\n\tAND ' + w

    deleteData = 'DELETE FROM [{}].[{}]'+deleteData + ';'
    # print(deleteData)
    engineCon(strCon).execute(
        sat(
            deleteData.format(schema, table)
        ).execution_options(autocommit=True)
    )


def depure(strCon, df, schema, table, depureColumns):

    depure = df.loc[:, depureColumns].drop_duplicates(subset=depureColumns)

    # print(depure.shape[0])
    for i in range(depure.shape[0]):
        where = []
        for k in depureColumns:
            w = '''{} = '{}' '''.format(k, depure.iloc[i][k])
            where.append(w)
        # print(where)

        deleteDataToSql(strCon, schema, table, where=where)


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
    columns = []
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

        columns.append(alchemy_col(c, str(data[c].dtype), largo))

        stament += '\t[{}] {}'.format(c, tipo)

    return ('''
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))
    CREATE TABLE [{}].[{}] (\n{})
    '''.format(schema, table, schema, table, stament), columns)


def createTable(strCon, schema, table, data, index=False):
    TableStament, Columns = createTableStament(data, schema, table, index)
    # print(TableStament)
    engineCon(strCon).execute(
        sat(
            TableStament
        ).execution_options(autocommit=True)
    )
    return Columns
