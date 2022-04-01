

from operator import contains
import os
import shutil
import re
from xlsx2csv import Xlsx2csv
import pandas as pd
from pylib.mod.error import packageForFileError
import os

from pylib.mod.utils import excutionTime, replaceIfContains


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


def blockExtractDataFile(path, files, sheets, firstRow=0):
    data = pd.DataFrame()
    for file in files:
        file_path = path + file
        if data is not None:
            data = pd.concat([data,
                              extractDataFile(file_path, file, sheets, firstRow)]
                             )

    return data


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
def extractDataFile(file_path, file, sheets, firstRow=0):
    try:
        data = pd.DataFrame()
        if contains(file_path, 'csv'):
            data = pd.read_csv(
                file_path, encoding='ISO-8859-1', sep=';')
        elif contains(file_path, 'xls'):
            for sheet in sheets:
                data = pd.concat([data,
                                  pd.read_excel(
                                      file_path,
                                      sheet_name=sheet,
                                      header=firstRow)
                                  ])
        else:
            return None

        data['source'] = file
        data = columnCleaner(data)
        data = trimAllColumns(data)

        return data
    except (ValueError, NameError):
        packageForFileError(
            file_path,
            "No se encuentra la pestaña {}.\n".format(sheet),
            file
        )
        return None
    except PermissionError:
        packageForFileError(
            file_path,
            "El archivo {}, esta en uso al momento de cargar.\n".format(file),
            file)
        return None
    except (TypeError) as e:
        packageForFileError(
            file_path,
            "Error: {}.\n".format(e),
            file)
        return None


def trimAllColumns(dataFrame):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    def trim_strings(x): return x.strip() if isinstance(x, str) else x
    return dataFrame.applymap(trim_strings)
