
import datetime as dt

from pylib.mod.utils import workDirectory


def packageForFileError(test, url, error, file):
    directory = workDirectory() + chr(92)+'Procesado' + chr(92)

    urllog = directory + dt.datetime.today().strftime("%Y%m%d")
    hour = dt.datetime.today().strftime("%H:%M")

    log = open(urllog + '.log', "a+", encoding="utf-8")
    print('log register: {}'.format(hour))

    log.write('{}, {}\t{}'.format(
        hour,
        ('Sin Archivo', file)[file is None],
        error
    ))
    log.close()
