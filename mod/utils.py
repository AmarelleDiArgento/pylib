
import re
import os
import json
from datetime import datetime as dt

from numpy import NAN, NaN


dev = True


def workDirectory():
    CURR_DIR = os.path.dirname(os.path.realpath(__file__))
    return removeRegexText(r'\\pylib\\mod', CURR_DIR)


def parameters(isTest=True):
    config_file = r'{}\util\config.json'.format(workDirectory())
    with open(config_file) as cdata:
        config = json.load(cdata)

    db_con = config['db_con']
    files = config['files']
    isTest = config['isTest']
    return (db_con, files, isTest)


def roundBy(x, base=1):
    if x is not NAN:
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
