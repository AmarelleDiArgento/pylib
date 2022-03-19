

import datetime as dt


def packageForFileError(url, error, file):
    urllog = r'' + url+'\Procesado' + \
        chr(92) + dt.datetime.today().strftime("%Y%m%d")
    hour = dt.datetime.today().strftime("%H:%M")

    log = open(urllog + '.log', "a+")
    # print(error)

    log.write('{}, {}\n{}'.format(hour, file, error))
    log.close()
