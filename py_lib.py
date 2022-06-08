
from itertools import groupby
import re
import os
import json
from datetime import datetime as dt

from operator import contains
import os
import shutil
import re
from typing import Literal
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
    return files


def parameters(ROOT, isTest=True, config_file='', service_name='general'):

    config_file = r'{}\util\{}'.format(
        ROOT, (config_file, 'config.json')[config_file == ''])

    with open(config_file) as cdata:
        config = json.load(cdata)
    isTest = config['isTest']

    db_con = config[service_name]['db_con']

    files = None
    if 'files' in config[service_name]:
        files = directories(ROOT, config[service_name]['files'], [
                            'storage', 'logs'])

    bulk_space = None
    if 'bulk_space' in db_con:
        bulk_space = db_con['bulk_space'] + chr(92)
        del db_con['bulk_space']

    return (db_con, files, isTest, bulk_space)


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


def removeColumnsIn(dataFrame, listToRemove=[], notIn=False, literal=False):
    # Eliminar columnas repetidas.
    pattern = '|'.join(listToRemove)
    if literal:
        dataFrame.drop(
            listToRemove,
            axis=1,
            inplace=True
        )
    else:
        dataFrame.drop(
            dataFrame.columns[
                (
                    dataFrame.columns.str.contains(pat=pattern, regex=True),
                    ~dataFrame.columns.str.contains(pat=pattern, regex=True)
                )[notIn]
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
#  Lib sqlAlchemy
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


def field_builder(fields, type_fields='S'):
    '''
    @fields: list of fields
    @type: S Select, G Group By, O Order By
    '''

    if type_fields == 'S' and fields is None:
        return '*'
    elif (type_fields == 'G' or type_fields == 'O') and fields is None:
        return ''
    else:

        pre = {
            'S': '[',
            'G': 'GROUP BY [',
            'O': 'ORDER BY ['
        }

        join = pre[type_fields]
        join = join + '], ['.join(fields)
        return join + ']'


def where_builder(where=[]):
    whereData = ''
    if where is not None:
        for w in where:
            if whereData == '':
                whereData += 'WHERE ' + w
                continue
            whereData += '\n\tAND ' + w
    else:
        whereData = ''
    return whereData


def query_builder(schema, table, fields=None, where=None, grouopby=None, orderby=None, limit=None):

    ifexist = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))\n'''.format(
        schema, table)

    query = ifexist + '\tSELECT {} {} FROM [{}].[{}] WITH(NOLOCK)'.format(
        ('Top {limit} ', '')[limit is None], field_builder(fields=fields, type_fields='S'), schema, table)
    query = query + '\n' + where_builder(where)
    query = query + '\n\t{}'.format(
        field_builder(fields=grouopby, type_fields='G'), schema, table)
    query = query + '\n\t{}'.format(
        field_builder(fields=orderby, type_fields='O'), schema, table)

    return query


def excecute_query(strCon, schema=None, table=None, query='', fields=None, where=None, groupby=None, orderby=None, limit=None):
    try:
        if query == '':
            query = query_builder(
                schema, table, fields, where, groupby, orderby, limit)
        data_source = engineCon(strCon).execute(
            sat(
                query
            ).execution_options(stream_results=True)
        )
        data_source = data_source.mappings().all()
        data_source = pd.DataFrame(data_source)
        data_source = trimAllColumns(data_source)

        return data_source

    except:
        print('Error en la consulta: \n{}'.format(query))
        return pd.DataFrame()


def affectedRows(func):
    def wrapper(*args, **kwargs):
        initialrows = rowCount(
            kwargs['strCon'], kwargs['schema'], kwargs['table'])
        results = func(*args, **kwargs)
        finalrows = rowCount(
            kwargs['strCon'], kwargs['schema'], kwargs['table'])
        affectedRows = finalrows - initialrows

        print('{} to {} affectedRows: {:,.1f}'.format(
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
def bulkInsert(strCon, schema, table, file_path, data, index=False):

    if data.empty == False:
        createTable(strCon, schema, table, data, index)

    ifexist = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))\n'''.format(
        schema, table)
    BULK = ifexist + \
        '''
        BULK INSERT [{}].[{}]
            FROM '{}' 
            WITH ( 
                DATAFILETYPE = 'char', 
                FIELDQUOTE = '"', 
                FIRSTROW = 2, 
                FIELDTERMINATOR = ';', 
                ROWTERMINATOR = '\\n', 
                TABLOCK 
            );
        '''.format(
            schema, table, file_path)
    # print(BULK)
    engineCon(strCon).execute(
        sat(
            BULK
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
        print(ex)
        raise ex
    except Exception as ex:  # Bad. >:[
        print(ex)
        raise ex


@affectedRows
def deleteDataToSql(strCon, schema, table, where=[]):

    ifexist = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{}].[{}]') AND type in (N'U'))\n'''.format(
        schema, table)
    where = where_builder(where)
    deleteData = '''
        {}
            DELETE FROM [{}].[{}]
                {}
        '''.format(ifexist, schema, table, where)
    # print(deleteData)
    engineCon(strCon).execute(
        sat(
            deleteData
        ).execution_options(autocommit=True)
    )


def depure(strCon, df, schema, table, depureColumns):
    # print(df)
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
        'bool': "bit",
        'datetime64[ns]': 'datetime',
        '<M8[ns]': 'NPI',
        'object': 'nvarchar'
    }

    columns = []
    stament = ''
    if index:
        stament = '\t[id{}] int identity(1,1)'.format(table)
    for c in columnName:

        if stament != '':
            stament += ',\n'
        tipo = datetype[str(data[c].dtype)]
        # print(c, tipo, str(data[c].dtype))
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


def add_new_element(strCon, schema, table, df_new_elements,
                    df_old_elements, column, prefix='id'):

    data = pd.DataFrame(df_new_elements[column].unique(), columns=[column])

    if df_old_elements.empty == False:
        data = pd.DataFrame(data, columns=[column])

        data = data.merge(df_old_elements, on=column,
                          how='left', indicator=True)
        data = data[data['_merge'] == 'left_only']

    if data.empty == False:
        data = removeColumnsIn(
            dataFrame=data,
            listToRemove=['_merge', prefix + column]
        )
        data = data.sort_values(column)

        insertDataToSql_Alchemy(strCon=strCon, schema=schema,
                                table=table, data=data, index=True)

    return excecute_query(
        strCon=strCon,
        schema=schema,
        table=table,
        fields=[prefix + column, column]
    )
