
import os
import re
import pandas as pd
from pylib.mod.error import packageForFileError


def searchFilesByContentInTitle(url, parm):
    allFilesInUrl = os.listdir(url)
    return [x for x in allFilesInUrl if(
        ~x.startswith('~$') &
        (re.search(parm['content'], x.lower()) is not None) &
        x.endswith(parm['ext'])
    )]


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


def blockExtractDataFile(url, files, sheet, firstRow=0):

    data = pd.DataFrame()
    for file in files:
        if data is not None:
            data = pd.concat([data,
                              extractDataFile(url, file, sheet, firstRow)]
                             )
    return columnCleaner(data)


def extractDataFile(url, file, sheet, firstRow=0):
    try:
        data = pd.read_excel(url + file, sheet, header=firstRow)
        data['Url'] = file
        #
        return data
    except (ValueError, NameError):
        #                   (url, error, file)
        packageForFileError(
            url,
            "No se encuentra la pestaña {}.\n".format(sheet),
            file
        )
        return None
    except PermissionError:
        packageForFileError(
            url,
            "El archivo {}, esta en uso al momento de cargar.\n".format(file),
            file)
        return None


def trimAllColumns(dataFrame):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    def trim_strings(x): return x.strip() if isinstance(x, str) else x
    return dataFrame.applymap(trim_strings)
