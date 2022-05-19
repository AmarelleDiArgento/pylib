
import re
import os
import json
from datetime import datetime as dt

from operator import contains
import os
import shutil
import re
from xlsx2csv import Xlsx2csv
import pandas as pd


import numpy as np
import pymssql as sql
import sqlalchemy as db
from sqlalchemy.orm import Session
from sqlalchemy import create_engine as sen, text as sat
from sqlalchemy.schema import Table,  MetaData, Column
from sqlalchemy.types import Integer, DateTime, String, Float, Boolean

import math


# ----------------------------------------------------------------------------------------------------
#  Lib utils
# ----------------------------------------------------------------------------------------------------


dev = True


def workDirectory():
    CURR_DIR = os.path.dirname(os.path.realpath(__file__))
    CURR_DIR = removeRegexText(r'\\pylib', CURR_DIR)
    # print(CURR_DIR)
    return CURR_DIR


def directories(ROOT, files, directories):
    for file in files:
        for directory in directories:
            root = (file['dir'], ROOT)[file['local']]

            new_directory = root + chr(92) + directory + chr(92)
            createDirectory(
                new_directory
            )
            file[directory] = new_directory
    # print(files)
    return files


def parameters(ROOT, isTest=True):
    # print(ROOT)
    config_file = r'{}\util\config.json'.format(ROOT)
    with open(config_file) as cdata:
        config = json.load(cdata)
    db_con = config['db_con']
    files = directories(ROOT, config['files'], ['storage', 'logs'])
    isTest = config['isTest']
    return (db_con, files, isTest)


def roundBy(x, base=1):
    if x is not None and pd.isna(x) == False:
        return int(base * round(float(x) / base))
    else:
        return 10
    # return int(base * round(float(x)/base))


def excutionTime(func):
    def wrapper(*args, **kwargs):
        # print('args:', args)
        if (dev):
            initial = dt.now()
            results = func(*args, **kwargs)
            final = dt.now()
            difference = final - initial
            # print('F {}: {}\nArgs: {}'.format(func.__name__, difference, args))
            print('f_{}: {}'.format(func.__name__, difference))
        return results

    return wrapper


def extractRegexText(params, text):

    pattern = re.compile(params)
    results = pattern.findall(text)
    if results:
        return results[0]
    else:
        return text


def removeRegexText(params, text):

    results = extractRegexText(params, text)
    if results:
        text = text.replace(results, '')

    return text


def replaceIfContains(text, oldText, newText):
    if text.lower().find(oldText.lower()) > 0:
        return newText
    else:
        return text


# ----------------------------------------------------------------------------------------------------
#  Lib Error
# ----------------------------------------------------------------------------------------------------


def packageForFileError(file_path, error, file):

    urllog = file_path + dt.today().strftime("%Y%m%d")
    hour = dt.today().strftime("%H:%M")

    log = open(urllog + '.log', "a+", encoding="utf-8")
    print('log register: {}'.format(hour))

    log.write('{}, {}\t{}'.format(
        hour,
        (file, 'Sin Archivo')[file is None],
        error
    ))
    log.close()


# ----------------------------------------------------------------------------------------------------
#  Lib Files
# ----------------------------------------------------------------------------------------------------

def createDirectory(url):
    if not os.path.exists(url):
        os.makedirs(url)


@excutionTime
def searchFilesByContentInTitle(file_path, parm):
    allFilesInUrl = os.listdir(file_path)
    return [x for x in allFilesInUrl if(
        ~x.startswith('~$') &
        (re.search(parm['content'].lower(), x.lower()) is not None) &
        x.endswith(parm['ext'])
    )]


def blockExtractDataFile(path, files, file):
    # sheets, firstRow=0, archive=True):
    data = pd.DataFrame()

    for file_name in files:
        if data is not None:
            data = pd.concat([data,
                              extractDataFile(path, file_name, file)]
                             )

    return data


def createDirectory(archive):
    if not os.path.exists(archive):
        os.makedirs(archive, exist_ok=True)


def archiveFile(path, file_name, file):
    src = path + file_name
    if os.path.exists(src):

        date = dt.now()
        day = date.strftime('%Y-%m-%d')
        hour = date.strftime('%H%M')
        dst = file['storage'] + chr(92) + day + chr(92)

        createDirectory(dst)
        dst = dst + hour + '_' + file_name
        # dst + chr(92) + hour + '_' + file

        shutil.move(src, dst)


def columnCleaner(dataFrame):
    # Eliminar columnas repetidas.
    dataFrame.drop(
        dataFrame.columns[
            dataFrame.columns.str.contains(pat='\.\d{1,2}', regex=True)
        ],
        axis=1,
        inplace=True
    )
# Eliminar columas sin nombre
    dataFrame.drop(
        dataFrame.columns[
            dataFrame.columns.str.contains('unnamed', case=False)
        ],
        axis=1,
        inplace=True
    )

    return dataFrame


def removeColumnsIn(dataFrame, listToRemove, notIn=False):
    # Eliminar columnas repetidas.

    pattern = '|'.join(listToRemove)

    if(~notIn):
        dataFrame.drop(
            dataFrame.columns[
                dataFrame.columns.str.contains(pat=pattern, regex=True)
            ],
            axis=1,
            inplace=True
        )
    else:
        dataFrame.drop(
            dataFrame.columns[
                ~dataFrame.columns.str.contains(pat=pattern, regex=True)
            ],
            axis=1,
            inplace=True
        )

    return dataFrame


def existsFile(filepath):
    return os.path.isfile(filepath)


def removeFile(filepath):
    os.remove(filepath)


def removeDirectory(url):
    try:
        if os.path.exists(url):
            shutil.rmtree(url)
    except OSError as e:
        print(f"Error:{ e.strerror}")


# @excutionTime
def convertXlsToCsv(url, file, sheets, isTest=False):
    filepath = url + file
    # print('Existe el archivo xls? {}'.format(existsFile(filepath)))
    if existsFile(filepath):
        if isTest:
            filepath = filepath.replace('xlsx', 'csv')
            # print('Existe el archivo csv? {}'.format(existsFile(filepath)))
            if existsFile(filepath) == False and existsFile(url+file) == True:
                Xlsx2csv(
                    url+file,
                    outputencoding="ISO-8859-1",
                    delimiter=';',
                    include_sheet_pattern=sheets
                ).convert(filepath, 0)
    else:
        return None

    return filepath


# @excutionTime
def extractDataFile(path, file_name, file):
    try:
        # print('LOGS', file)
        file_path = path + file_name
        data = pd.DataFrame()
        if contains(file_path, 'csv'):
            data = pd.read_csv(
                file_path, encoding='ISO-8859-1', sep=';')
        elif contains(file_path, 'xls'):
            for sheet in file['sheets']:
                # print('file_path', file_path, 'sheet', sheet)
                data = pd.concat([data,
                                  pd.read_excel(
                                      file_path,
                                      sheet_name=sheet,
                                      header=file['firstRow'])
                                  ])
        else:
            return None

        data['source'] = file_name
        data = columnCleaner(data)
        data = trimAllColumns(data)

        archiveFile(path, file_name, file)
        return data
    except (ValueError, NameError):
        packageForFileError(
            file['logs'],
            "No se encuentra la pestaña {}.\n".format(sheet),
            file_name
        )
        return None
    except PermissionError:
        packageForFileError(
            file['logs'],
            "El archivo {}, esta en uso al momento de cargar.\n".format(
                file_name
            ),
            file_name
        )
        return None
    except (TypeError) as e:
        packageForFileError(
            file['logs'],
            "Error: {}.\n".format(e),
            file_name
        )
        return None


def trimAllColumns(dataFrame):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    def trim_strings(x): return x.strip() if isinstance(x, str) else x
    return dataFrame.applymap(trim_strings)


# ----------------------------------------------------------------------------------------------------
#  Lib SQL
# ----------------------------------------------------------------------------------------------------


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


@affectedRows
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
    # print(depureColumns)
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
