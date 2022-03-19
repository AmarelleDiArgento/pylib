
import re
from datetime import datetime as dt


dev = True


def excutionTime(func):
    def wrapper(*args, **kwargs):
        if (dev):
            initial = dt.now()
            results = func(*args, **kwargs)
            final = dt.now()
            difference = final - initial
            # print('F {}: {}\nArgs: {}'.format(func.__name__, difference, args))
            print('F {}: {}'.format(func.__name__, difference))
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
