

import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat
from sqlalchemy.sql.functions import concat


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


def insertDataToSqlDirty(srtCon, schema, table, data):
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


def deleteDataToSql(srtCon, schema, table, df, dateCol):
    tmp = pd.DataFrame(df[dateCol])
    tmp['Year'] = tmp[dateCol].apply(lambda x: str(x.year))
    year = tmp['Year'].unique()

    year = "', '".join(year)
    DeleteDataTable = """
            DELETE FROM {}.{}
            WHERE YEAR({}) IN ('{}')
            """

    engineCon(srtCon).execute(
        sat(
            DeleteDataTable.format(
                schema,
                table,
                dateCol,
                year
            )
        ).execution_options(autocommit=True)
    )


def selectDataToSql(srtCon, schema, table, df, dateCol):

    year = "', '".join(year)
    DeleteDataTable = """
            SELECT * FROM {}.{}
            """

    engineCon(srtCon).execute(
        sat(
            DeleteDataTable.format(
                schema,
                table
            )
        ).execution_options(autocommit=True)
    )
