
import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat

dbProjCon = {
    'Test': {
        'server': 'elt-dbproj-fca\\testing',
        'db': 'Logistica',
        'user': 'talend',
        'password': 'S3rvT4l3nd*',
        'tables': {
            'table': {'schema': '',
                      'name': ''}
        }
    }}
