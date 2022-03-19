from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from googletrans import Translator
import os
import io
import re
import pandas as pd
import numpy as np
import datetime as dt
import json




english = True
isTest = False
anio = None
config = None

dicc = None

CONFIG = None




"""
OPTENER USUARIO Y CONTRASEÑA DE VARIABLES DE ENTORNO
"""


def dataByConfig(isTest):
    global config
    use = {} | (config["deploy"], config["test"])[isTest]
    return use


def getCredentials(isTest):
    global config
    with open(CONFIG) as cdata:
        config = json.load(cdata)

    data = dataByConfig(isTest)
    # print('data:', data)
    return {

        "repository": config["repository"],
        "USERNAME": os.environ.get(data["user"]),
        "PASSWORD": os.environ.get(data["passwd"]),
        "collection": data["collection"],
        "subCollection": data["subCollection"],
        "files": data["files"]

    }


def getDataFile(objet, name):
    return objet["files"][name]


"""
VARIABLES DE BIBLIOTECA, RUTA DEL ARCHIVO Y ARCHIVO
GENERAR URI (FULL PATH)
"""


def getUrlRepository(credentials):
    return '{}/sites/{}/{}'.format(
        credentials["repository"],
        credentials["collection"],
        credentials["subCollection"]
    )


def getDirectoryUrl(credentials, file):

    path = '/sites/{}/{}/Documentos/{}'.format(
        credentials["collection"],
        credentials["subCollection"],
        getDataFile(credentials, file)["file_url"]
    )
    print("dirPath: ", path)
    return path


def getFullUrl(credentials, file):
    path = '/sites/{}/{}/Documentos/{}/{}'.format(
        credentials["collection"],
        credentials["subCollection"],
        file["file_url"],
        file["file"]
    )
    print("urlPath: ", path)
    return path


"""
VALIDAR CONEXION CON Y PERMISOS A LA UNIDAD SOLICITADA
"""


def getContextAuth(credentials, url):
    context_auth = AuthenticationContext(url)
    context_auth.acquire_token_for_user(
        credentials["USERNAME"],
        credentials["PASSWORD"]
    )

    return ClientContext(url, context_auth)


def sharePointConnectTest(ctx):
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print("Connected with Web site: {}".format(web.properties['Title']))


"""
RETORAN LAS PROPIEDADES DEL ARCHIVO SOLICITADO
"""


def propertiesFile(fullUrl, ctx):
    """
      ACCEDER AL ARCHIVO Y RETORNAR SUS PROPIEDADES
    """
    file: File = ctx.web.get_file_by_server_relative_url(fullUrl)
    ctx.load(file)
    ctx.execute_query()

    print('File name: %s' % file.properties['Name'])
    print('Creation date: %s' % file.properties['TimeCreated'])
    print('Modification date: %s' % file.properties['TimeLastModified'])
    print('Version: %s' % file.properties['UIVersion'])
    print('Version label: %s' % file.properties['UIVersionLabel'])
    print('')

    # print(json.dumps(file.properties))

    return {
        "Uri": fullUrl,
        "FileName": file.properties['Name'],
        "FileUrl": file.properties['LinkingUrl'],
        "Creation date": file.properties['TimeCreated'],
        "Modification date": file.properties['TimeLastModified'],
        "Version": file.properties['UIVersion'],
        "Version label": file.properties['UIVersionLabel'],
        "isNewVersion": True}


"""
DESCARGA UN ARCHIVO DE SHAREPOINT Y LO ALAMCENA EN LA CARPETA FILES
"""


def downloadFile(ctx, properties):
    path = os.path.join("./files/", properties["FileName"])
    with open(path, "wb") as local:
        file = ctx.web.get_file_by_server_relative_path(
            properties["Uri"])

        file.download(local).execute_query()

    print("[Ok] file has been downloaded into: {0}".format(path))


"""
LEE UN ARCHIVO DE SHAREPOINT Y LO ALAMCENA EN LA CARPETA FILES
DESCARGA EL CONTENIDO DE TODAS LAS PETAÑAS EN UN DATAFRAME
NO GENERA MODIFICACIONES
"""


def readFileInCloud(ctx, properties):
    response = File.open_binary(ctx,   properties["Uri"])

    # save data to BytesIO stream
    bytesFileObj = io.BytesIO()
    bytesFileObj.write(response.content)
    bytesFileObj.seek(0)  # set file object to start

    # read file into pandas dataframe
    # dateparse = lambda x: dt.datetime.strptime(x, '%m-%d-%Y %H:%M:%S')
    df = pd.read_excel(bytesFileObj, sheet_name=None)
    data = pd.DataFrame()
    for name, sheet in df.items():
        # print(name)
        data = data.append(sheet)

    data = translateColunm(data, 'CONCEPTO ')
    data['CONCEPTO '] = data['CONCEPTO '].str.upper()
    return data


"""
LEE UN ARCHIVO DE SHAREPOINT Y LO ALAMCENA EN LA CARPETA FILES
RETORNA:
    DATA:
        DESCARGA EL CONTENIDO DE TODAS LAS PETAÑAS EN UN DATAFRAME, SALVO POR LAS PESTAÑAS.
        FILA INICIAL 6
            * CONSOLIDADO
            * Sheet2
            * BY ACCOUNT

        GENERA MODIFICACIONES:
            * REMUEVE COLUMNAS SIN NOMBRE
            * REMUVE COLUMNA TOTAL
            * REMUEVE VALORES VACIOS Y NULOS EN "No." O QUE NO CONTENGAN ESTRUCTURA 1. (Numero seguido punto)
    PRINCIPAL:
        DESCARGA EL CONTENIDO DE LA PETAÑA CONSOLIDADO EN UN DATAFRAME
        FILA INICIAL 4

        GENERA MODIFICACIONES:
            * REMUEVE COLUMNAS SIN NOMBRE
            * REMUVE COLUMNA TOTAL
            * REMUEVE VALORES VACIOS Y NULOS EN "No." O QUE NO CONTENGAN ESTRUCTURA 1. (Numero seguido punto)

"""


def readFileInCloudBudgetError(ctx, properties):
    response = File.open_binary(ctx,   properties["Uri"])

    # save data to BytesIO stream
    bytesFileObj = io.BytesIO()
    bytesFileObj.write(response.content)
    bytesFileObj.seek(0)  # set file object to start
    # read file into pandas dataframe
    df = pd.read_excel(bytesFileObj, sheet_name=None,
                       skiprows=6, index_col=None)

    data = pd.DataFrame()
    principal = pd.DataFrame()
    for name, sheet in df.items():
        # name != "CONSOLIDADO" and name != "CONS1" and name != "Sheet2" and name != "BY ACCOUNT":
        if re.search('(((\d\.){1,})(\d){0,})', name) is not None:
            # print(name)
            # sheet = sheet.rename(columns={
            #     'VOLVER': 'No.',
            #     'Unnamed: 1': 'CONCEPTO',
            #     'Unnamed: 3': 'ENE',
            #     'Unnamed: 4': 'FEB',
            #     'Unnamed: 5': 'MAR',
            #     'Unnamed: 6': 'ABR',
            #     'Unnamed: 7': 'MAY',
            #     'Unnamed: 8': 'JUN',
            #     'Unnamed: 9': 'JUL',
            #     'Unnamed: 10': 'AGO',
            #     'Unnamed: 11': 'SEP',
            #     'Unnamed: 12': 'OCT',
            #     'Unnamed: 13': 'NOV',
            #     'Unnamed: 14': 'DIC'
            # })
            # print(sheet)
            sheet = sheet.loc[:, ~sheet.columns.str.contains('^Unnamed')]
            sheet = sheet.loc[:, ~sheet.columns.str.contains('^TOTAL')]
            sheet = sheet[sheet["No."] != ""]
            sheet = sheet[sheet["No."].apply(
                lambda x:  re.search('\d\.', str(x)) is not None)]
            sheet = sheet.dropna(subset=["No."])
            data = data.append(sheet)
    # print(data, principal)
    data['CONCEPTO'] = data['CONCEPTO'].str.upper()

    principal = pd.read_excel(bytesFileObj, sheet_name="CONSOLIDADO",
                              skiprows=4)
    principal = pd.DataFrame(principal, columns=['CONCEPTO', 'Cuenta'])
    principal['CONCEPTO'] = principal['CONCEPTO'].str.upper()
    principal = translateColunm(principal, 'Cuenta')
    principal = principal.dropna(subset=["CONCEPTO"])
    principal = principal[principal["CONCEPTO"].apply(
        lambda x:  re.search('\d\.', str(x)) is not None
    )]

    return (data, principal)


"""
LEER UN DATAFRAME, COLUMNAS DE MES:
    GENERA MODIFICACIONES:
        * TRASPONE LOS MESES
        * REUBICA No. COMO ID AL INICIO DE CADA FILA
        * FILTRA VALORES MAYORES A 0

"""


def processDetailsBudget(df, year):
    # print(df.columns)
    col = ['No.', 'ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
           'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    dfDetails = df.loc[:, col]
    dfDetails = dfDetails.T

    size = dfDetails.shape
    # print(size, dfDetails)

    d = []
    detalles = dfDetails.to_numpy()
    # print(size, dfDetails, detalles)

    for i in range(1, size[0]):

        for j in range(0, size[1]):
            d.append([detalles[0][j], col[i], detalles[i][j]])
    dfDetails = pd.DataFrame(d, columns=['No.', 'Month', 'Price'])
    dfDetails = dfDetails[dfDetails['Price'] > 0]
    dfDetails['Month'] = dfDetails['Month'].apply(
        lambda x: changeMonthName(x, year))
    # print(dfDetails)
    return dfDetails


"""
METODO PARA LAMBDAS, RETORNA CONCEPTO SEGUN LA POSICION EN LA JERARQUIA, SI CONCUERDA CON EL VALOR RETORNA CONCEPTO, SI NO RETORNA NA
REQUERIDO EN FUNCION:
    * createLvlConcept
"""


def insertConceptInRowColum(row, i):
    if row['Row'] == i:
        return row['CONCEPTO']
    else:
        return None


"""
RECIBE UN ENCABEZADO Y DIVIDE EL No. EN INDICES (SEGUN UBICACION DE PUNTO .)
    GENERA MODIFICACIONES:
        * CREAR IDS POR CADA LVL
        * LIMPIA COLUMNAS VACIAS
        * CREA COLUMNA ROW, CUENTA COLUMNAS VACIAS (SE USA PARA GENERAR LAS JERARQUIA, MENOS VACIAS MENOR JERARQUIA)
        * ASIGNA CONCEPTO EN EL NIVEL ASIGNADO
        * ORDENA EL DATAFRAME
        * REINICIA EL ID
        * RELLENA COLUMNAS CON EL VALOR DE LA ULTIMA FILA LLENA
        * LIMPIA JERARQUIAS SIN SUBNIVEL
"""


def createLvlConcept(df):
    # Dividir No.
    tabs = df["No."].str.split('.', n=3, expand=True)
    # Crear ids
    for i in range(0, 4):
        df['lvl{}_id'.format(i+1)] = tabs[i]
    # Remplazar vacios por NA
    for i in df.columns:
        df[i][df[i].apply(lambda i: True if re.search(
            '^\s*$', str(i)) else False)]=None
    # Contar columnas vacias
    col = ['lvl1_id', 'lvl2_id', 'lvl3_id', 'lvl4_id']
    dfLvl = df.loc[:, col]
    df['Row'] = 4-dfLvl.isnull().sum(axis=1)
    # Registrar concepto en el nivel asignado
    for i in range(1, 5):
        df['lvl{}'.format(i)] = df.apply(
            lambda x:   insertConceptInRowColum(x, i), axis=1)
    # ordenar el dataframe por No.
    df = df.sort_values('No.')

    # Reiniciar index (Consecutivo)
    df.reset_index(drop=True, inplace=True)

    # Rellenar filas vacias con su Concepto superior (control + j en excel)
    df = df.fillna(axis=0, method='ffill')

    # Limpiar primeras jerarquias sin sub nivel
    for i in df.index:
        row = df['Row'][i]
        for j in range(1, 5):
            if row < j:
                df['lvl{}'.format(j)][i] = None
    return df


"""
CREA EL ENCABEZADO Y EDITA AL LLAMAR A LA FUNCION:
    * createLvlConcept
    GENERA MODIFICACIONES:
        * UNE ENCABEZADO CON ID PRINCIPALES
        * LIMPIA LA COLUMNA CUENTA
"""


def processHeaderBudget(df, dfPrincipal):
    # print(df.columns)
    col = ['No.', 'CONCEPTO']
    dfHeader = df.loc[:, col]

    dfHeader["Cuenta"] = ""
    dfHeader = dfHeader.append(dfPrincipal)
    # dfHeader['Cuenta'] = dfHeader['Cuenta'].apply(
    #     lambda x:  str(x).lower().replace('·', ' ')
    # )

    aco = dfHeader['Cuenta'].str.split(' ', n=1, expand=True)
    dfHeader['Cuenta'] = aco[0]
    dfHeader['Cuenta'] = dfHeader['Cuenta'].apply(
        lambda x:  str(x).lower().replace(' ', '')
    )

    dfHeader = translateColunm(dfHeader, 'CONCEPTO')
    dfHeader = createLvlConcept(dfHeader)

    # dfHeader = pd.concat([dfHeader, tabs])

    # dfHeader['lvl1'] = tabs['lvl1']
    # dfHeader['lvl2'] = tabs['lvl2']
    # dfHeader['lvl3'] = tabs['lvl3']
    # dfHeader['lvl4'] = tabs['lvl4']
    # dfHeader['row'] = tabs['Row']

    return dfHeader


"""
METODO PARA LAMBDAS, RETORNA LA FECHA SEGUN EL MES SUMINISTRADO, SI NO RETORNA NA
"""


def changeMonthName(date, anio):

    mount = {
        'ENE': dt.datetime(anio, 1, 1),
        'FEB': dt.datetime(anio, 2, 1),
        'MAR': dt.datetime(anio, 3, 1),
        'ABR': dt.datetime(anio, 4, 1),
        'MAY': dt.datetime(anio, 5, 1),
        'JUN': dt.datetime(anio, 6, 1),
        'JUL': dt.datetime(anio, 7, 1),
        'AGO': dt.datetime(anio, 8, 1),
        'SEP': dt.datetime(anio, 9, 1),
        'OCT': dt.datetime(anio, 10, 1),
        'NOV': dt.datetime(anio, 11, 1),
        'DIC': dt.datetime(anio, 12, 1),
        '': None
    }

    return mount[date]


"""
TRADUCE LA COLUMNA POR PARAMETRO SI LA VARIABLE GLOBAL english ES TRUE
"""


def chargeDic():

    global dicc
    with open('./files/dic.json') as cdata:
        dicc = json.load(cdata)


def updateDic(sentence):

    global dicc
    dicc = dicc | sentence

    with open('./files/dic.json', 'w') as outfile:
        outfile.write(json.dumps(dicc, indent=4))


def translateColunm(df, column):

    global english
    if english:

        df[column] = df[column].apply(
            lambda x:  translate(x))

    return df


"""
TRADUCE UN TEXTO DE ESPAÑOL A INGLES
"""


def translate(text):
    if text is not None and pd.isnull(text) == False and text != 'nan':
        text = text.strip().upper()
        global dicc
        try:
            test = dicc[text]
            return test
        except:

            translator = Translator()
            result = translator.translate(
                text, src='es', dest='en')
            result = result.text
            updateDic({
                text: result.strip().upper()
            })
            return result
        # print(f'{text} => {result}')
        # return str(result).strip().upper()


"""
EDITA LA PESTAÑA CONCEPTO.
    GENERA MODIFICACIONES:
        * DIVIDE EL CONCEPTO EN ID Y CONCEPTO
"""


def processPrincipalBudget(df):

    # df['CONCEPTO'] = df['CONCEPTO'].apply(
    #     lambda x:  translate(x))
    # print(df.columns)

    new = df['CONCEPTO'].str.split(' ', n=1, expand=True)
    df['No.'] = new[0]
    df['CONCEPTO'] = new[1]

    # print(df)
    return df


def fileProcess(credentials, file, ctx):
    urlFiley = getFullUrl(credentials, file)
    return propertiesFile(urlFiley, ctx)


def updateVersionFile(file, properties):
    return {
        "file_url": file["file_url"],
        "file": properties["FileName"],
        "version": properties["Version"],
        "versionLabel": properties["Version label"]
    }


def isNewFile(file, properties):
    return file['version'] != properties["Version"]


def runProcessShop(credentials, ctx):
    name = 'shopping'
    upVersion = {}
    df = pd.DataFrame()

    for year, file in credentials['files'][name].items():

        properties = fileProcess(credentials, file, ctx)

        upVersion = upVersion | {year: updateVersionFile(file, properties)}

        if isNewFile(file, properties):
            df = df.append(readFileInCloud(ctx, properties))

    if len(df) > 0:
        df = df.rename(columns={
            'FECHA ': 'DateF',
            'FACTURA ': 'Invoice',
            'TOTAL': 'Total',
            'BODEGA': 'Warehouse',
            'RAZON SOCIAL ': 'BussinesName',
            'CUENTA  PPTO': 'Account',
            'CONCEPTO ': 'Concept',
            'PROVEEDOR': 'Supplier'
        })

        df['DateF'] = df['DateF'].apply(lambda x: dateError(x))
        df = createSuffixAndCleanAccount(df)
        srtCon = stringConnect(dbProjCon)
        deleteDataToSql(srtCon, schema, tables[1], df, 'DateF')
        insertDataToSql(srtCon, schema, tables[1], df)
        return upVersion
    else:
        print(name + ' without changes')
        print('')
        # df.to_excel("E:\RECURSO_COMPARTIDO_TALEND/result.xlsx")


def dateError(eDate):

    if type(eDate) == str:
        symbol = None
        if re.search('/', eDate) is not None:
            symbol = '/'
        elif re.search('-', eDate) is not None:
            symbol = '-'

        eDate = eDate.split(' ')
        splitDate = eDate[0].split(symbol)
        dateReal = dt.datetime(int(splitDate[2]), int(
            splitDate[0]), int(splitDate[1]))

        # print(eDate, dateReal)
        return dateReal
    else:
        # dateReal = dt.datetime(eDate.year,eDate.day,eDate.month)
        # print(eDate, dateReal)
        # return dateReal
        return eDate


def createSuffixAndCleanAccount(df):

    df['Account'] = df['Account'].apply(
        lambda x: str(x).replace(' ', '')
    )

    df['Suffix'] = df['Account'].apply(
        lambda x: suffixOfAccount(x)
    )

    df['Account'] = df.apply(
        lambda x: cleanAccount(x), axis=1
    )
    return df


def suffixOfAccount(txt):
    txt = str(txt)
    if txt is not None and len(txt) > 0:
        if re.search('(\d{4})', txt) is not None:
            return re.sub('(\d{4})', '', txt)
    return None


def cleanAccount(row):
    # print(aco)

    if str(row['Suffix']) is not None:
        return re.sub(str(row['Suffix']), '', str(row['Account']))
    else:
        return str(row['Account'])


def runProcessBudget(credentials, ctx):

    name = 'budget'
    upVersion = {}
    dfm = pd.DataFrame()

    for year, file in credentials['files'][name].items():

        properties = fileProcess(credentials, file, ctx)

        upVersion = upVersion | {year: updateVersionFile(file, properties)}

        if isNewFile(file, properties):
            df = readFileInCloudBudgetError(ctx, properties)
            dfDetails = processDetailsBudget(df[0], int(year))
            dfPrincipal = processPrincipalBudget(df[1])
            dfHeader = processHeaderBudget(df[0], dfPrincipal)
            dfMerge = pd.merge(dfHeader, dfDetails)
            dfm = dfm.append(dfMerge)

    if len(dfm) > 0:
        srtCon = stringConnect(dbProjCon)
        dfm = dfm.rename(columns={'CONCEPTO': 'Concept',
                                  'Cuenta': 'Account', 'No.': 'ID', 'Month': 'datePre'})
        dfm = createSuffixAndCleanAccount(dfm)

        col = ['ID', 'Concept', 'Account', 'Suffix', 'lvl1',
               'lvl2', 'lvl3', 'lvl4', 'datePre', 'Price']
        dfm = dfm.loc[:, col]

        # print(dfm)

        deleteDataToSql(srtCon, schema, tables[0], dfm, 'datePre')
        insertDataToSql(srtCon, schema, tables[0], dfm)

        return upVersion
    else:
        print(name + ' without changes')
        print('')


def editConfFile(isTest, name, params):

    if params is not None:

        global config

        typeAct = ("deploy", "test")[isTest]
        cnf = config.copy()
        files = cnf[typeAct]['files']

        for year, param in params.items():
            files[name][year] = param

        config[typeAct]['files'] = files

        with open(CONFIG, 'w') as outfile:
            outfile.write(json.dumps(config, indent=4))


def yearList():
    return [str(x) for x in range(2021, dt.date.today().year+2)]


def run():
    global isTest
    chargeDic()
    credentials = getCredentials(isTest)
    urlRepository = getUrlRepository(credentials.copy())
    ctx = getContextAuth(credentials, urlRepository)
    sharePointConnectTest(ctx)

    editConfFile(
        isTest,
        'shopping',
        runProcessShop(credentials, ctx)
    )

    editConfFile(
        isTest,
        'budget',
        runProcessBudget(credentials, ctx)
    )

    #
# dfMerge.to_excel("E:\RECURSO_COMPARTIDO_TALEND/result.xlsx")


def label(txt):    
    """Resuelve una ecuación cuadrática.

    Devuelve en una tupla las dos raíces que resuelven la
    ecuación cuadrática:
    
        ax^2 + bx + c = 0.

    Utiliza la fórmula general (también conocida
    coloquialmente como el "chicharronero").

    Parámetros:
    a -- coeficiente cuadrático (debe ser distinto de 0)
    b -- coeficiente lineal
    c -- término independiente

    Excepciones:
    ValueError -- Si (a == 0)
    
    """

    with open('./files/config.log', 'a') as outfile:
        outfile.write('{}!: {}\n'.format(txt, dt.datetime.now()))

    print('')
    print('--------------------------------------------------------------------------------------------------')
    print('{}!:'.format(txt), dt.datetime.now())
    print('--------------------------------------------------------------------------------------------------')
    print('')


# urlDirectoryBudget = getDirectoryUrl(credentials, 'budget')
# print(urlDirectoryShops, urlDirectoryBudget)
if __name__ == '__main__':
    label('Run')
    run()
    label('End')


# $ pip uninstall googletrans
# $ git clone https://github.com/alainrouillon/py-googletrans.git
# $ cd ./py-googletrans
# $ git checkout origin/feature/enhance-use-of-direct-api
# $ python setup.py install

# User: pptousa@eliteflower.com
# Pass: PPTOmia21
