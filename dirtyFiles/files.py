

from numpy import NaN
import pandas as pd
import os
import re
import datetime as dt
# Maneno de errores
import errno
import sys


import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat

url = 'E:/RECURSO_COMPARTIDO_LOGISTICA/'


dbProjCon = {
    'server': 'elt-dbproj-fca\\testing',
    'db': 'Logistica',
    'user': 'talend',
    'password': 'S3rvT4l3nd*',
}

schema = 'tmp'
table = 'diponible'


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


def deleteDataToSql(srtCon, schema, table, master):
    master = "', '".join(master)
    DeleteDataTable = """
            DELETE FROM {}.{}
            WHERE CAST(fecha AS DATE) = CAST(GETDATE() AS DATE) AND
            Maestro IN ('{}')
            """

    engineCon(srtCon).execute(
        sat(
            DeleteDataTable.format(
                schema,
                table,
                master
            )
        ).execution_options(autocommit=True)
    )


def searchFiles(url, parm):
    allFilesInUrl = os.listdir(url)
    return [x for x in allFilesInUrl if(~x.startswith('~$') & (re.search(parm['content'], x.lower()) is not None) & x.endswith(parm['ext']))]


def extractDataFile(url, file, sheet):
    print(url, file, sheet)
    try:
        url = url + file

        return pd.read_excel(url, sheet)
    except (ValueError, NameError):
        packageForError("No se encuentra la pestaña {}.\n".format(sheet), file)
    except PermissionError:
        packageForError(
            "El archivo {}, esta en uso al momento de cargar.\n".format(file), file)


def cleanExistingRecords(MasterProducts):
    srtCon = stringConnect(dbProjCon)
    deleteDataToSql(srtCon, schema, table, MasterProducts)


def insertDataToSql(srtCon, schema, table, data):
    data.to_sql(
        schema=schema,
        name=table,
        con=engineCon(srtCon),
        if_exists='append',
        index=False
    )


def addInventory(srtCon, schema, date):
    try:
        ExcInventory = """
                EXEC {}.[AjustarInventarios] '{}'
                """

        engineCon(srtCon).execute(
            sat(
                ExcInventory.format(
                    schema,
                    date
                )
            ).execution_options(autocommit=True)
        )
    except (sal.exc.OperationalError, sal.exc.ProgrammingError) as e:
        packageForSQLError(e, 'PA AddInventory.xlsx')


def packageForSQLError(error, file):
    log = open(url + file.replace('xlsx', 'log'), "a")
    error = error.args[0].replace('\\n', '\n')
    error = error.replace(') (', '\n')
    error = error.replace('(', '').replace(')', '').replace("'", '')
    content = f'El Archivo {file}, no pudo ser procesado.'
    content = content + '\n' + str(error)
    content = content + '\n' + 'A MSSQLDatabaseException has been caught.'
    print(content)
    log.write(content)
    log.close()


def packageForError(error, file):
    print(error)
    global url

    urllog = url+'Procesado/' + dt.datetime.today().strftime("%Y%m%d")
    hour = dt.datetime.today().strftime("%H%M")

    log = open(urllog + '/error.log', "a")
    # print(error)

    log.write('{}         {}        {}\n'.format(hour, file, error))
    log.close()


# PermissionError


def moveProcessedFile(file):
    dir = url + 'Procesado/' + dt.datetime.today().strftime("%Y%m%d")
    try:
        os.mkdir(dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    src = url + file
    des = dir + '/' + dt.datetime.today().strftime("%H%M") + '_' + file
    os.rename(src, des)


def clearDataFile(dataFile, file, date):
    try:
        dataFile = dataFile.loc[:, ~dataFile.columns.str.match('Unnamed')]
        dataFile['Fecha'] = date
        dataFile = dataFile[~dataFile['Maestro'].str.contains('total')]
        dataFile = pd.DataFrame(dataFile,
                                columns=['Maestro', 'Semana', 'Producto', 'Item', 'Color',
                                         'Variedad', 'Grado', 'Prioridad', 'Especial', 'Tallos', 'Fecha']
                                )
        return dataFile
    except TypeError:
        # print(te)
        packageForError("Columna con nombre invalido", file)


def cycleProcessFiles(files, date, srtCon):

    for file in files:
        try:
            dataFile = extractDataFile(url, file)
            if dataFile is not None:
                dataFile = clearDataFile(dataFile, file, date)
                if dataFile is not None:
                    masterProducts = dataFile['Maestro'].unique()
                    deleteDataToSql(srtCon, schema, table, masterProducts)
                    insertDataToSql(srtCon, schema, table, dataFile)
                    moveProcessedFile(file)

        except (sal.exc.OperationalError, sal.exc.ProgrammingError) as e:
            packageForSQLError(e, file)


def run():
    srtCon = stringConnect(dbProjCon)
    files = searchBalanceFiles(url)
    if(files):
        date = dt.datetime.today().strftime("%Y-%m-%d")
        cycleProcessFiles(files, date, srtCon)
        addInventory(srtCon, 'dbo', date)

    else:
        print("there's nothing to do")


if __name__ == '__main__':
    run()


# ValueError: Worksheet named 'Base' not found
