#!/usr/bin/env python3
import argparse # https://docs.python.org/3/library/argparse.html
import datetime
import http.client
import json
import requests
import sqlite3

from datetime import datetime
from dateutil.relativedelta import relativedelta

# HOST = 'apis.data.go.kr'
# PORT = 80
# PATH = '/B090041/openapi/service/SpcdeInfoService'
_URL = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService'

_DB = './db/sc-oa-09-04.db'
_TABLE = 'sc_oa_09_04'

parser = argparse.ArgumentParser()
parser.add_argument('--service-key', dest='serviceKey', help='?serviceKey', required=True)
parser.add_argument('--end-year', dest='endYear', type=int, required=True)
parser.add_argument('--start-year', dest='startYear', type=int, required=False)
args = parser.parse_args()
if args.startYear is None:
    args.startYear = datetime.today().year

service_key = args.serviceKey
start_year = args.startYear
end_year = args.endYear
# print(service_key)
# print(end_year)
# print(start_year)


class Info:
    __date = None
    def __init__(self,
                 ___sol_year, ___sol_month,
                 __locdate, __date_kind, __date_name, __seq, __is_holiday, __remarks, __kst, __sun_longitude
                 ):
        self.sol_year = ___sol_year
        self.sol_month = ___sol_month
        self.locdate = __locdate
        self.date_kind = __date_kind
        self.date_name = __date_name
        self.seq = __seq
        self.is_holiday = __is_holiday
        self.remarks = __remarks
        self.kst = __kst
        self.sun_longitude = __sun_longitude
        self.__date = datetime.strptime(self.locdate, '%Y%m%d')
        self.year = self.__date.year
        self.month = self.__date.month
        self.day = self.__date.day



def get(operation_name, sol_year):
    payload = {'serviceKey': service_key, 'solYear': sol_year, '_type': 'json', 'numOfRows': 1024}
    r = requests.get(_URL + '/' + operation_name, params=payload)
    return r.json()


def get_holi_de_info(___sol_year):
    d = get('getHoliDeInfo', ___sol_year)
    response = d.get('response', {})
    body = response.get('body', {})
    items = body.get('items', {})
    if type(items) is not dict:
        print('not a dict!')
        return
    i = items.get('item', {})
    for item in i:
        info = Info(
            ___sol_year, None,
            str(item['locdate']),
            item['dateKind'],
            item['dateName'],
            item.get('seq', None),
            item.get('isHoliday', None),
            item.get('remarks', None),
            item.get('kst', None),
            item.get('sunLongitude', None)
        )
        insert(info)


def get_rest_de_info(___sol_year):
    d = get('getRestDeInfo', ___sol_year)
    response = d.get('response', {})
    body = response.get('body', {})
    items = body.get('items', {})
    if type(items) is not dict:
        print('not a dict!')
        return
    i = items.get('item', {})
    for item in i:
        info = Info(
            ___sol_year, None,
            str(item['locdate']),
            item['dateKind'],
            item['dateName'],
            item.get('seq', None),
            item.get('isHoliday', None),
            item.get('remarks', None),
            item.get('kst', None),
            item.get('sunLongitude', None)
        )
        insert(info)


def delete( ___sol_year, __date_kind):
    connection = sqlite3.connect(_DB)
    try:
        query = """
        DELETE FROM %s WHERE ___sol_year = '%s' AND __date_kind = '%s'
        """ % (_TABLE, ___sol_year, __date_kind)
        print(query)
        cursor = connection.cursor()
        cursor.execute(query)
        rowcount = cursor.rowcount
        print(f"number of deleted rows for {___sol_year}, {__date_kind}: ", rowcount)
        connection.commit()
    finally:
        connection.close()
        print("connection closed.")


def insert(info):
    connection = sqlite3.connect(_DB)
    try:
        query = """
        INSERT INTO %s (
        ___sol_year, ___sol_month,
        __locdate, __date_kind, __date_name, __seq, __is_holiday, __remarks,
         __kst, __sun_longitude,
        _year, _month, _day
        ) VALUES (
        '%s', '%s',
        '%s', '%s', '%s', %d, '%s', '%s',
        '%s', '%s',
        %d, %d, %d
        )
        """ % (_TABLE,
               info.sol_year, info.date_kind,
               info.locdate, info.date_kind, info.date_name, info.seq, info.is_holiday, info.remarks,
               info.kst, info.sun_longitude,
               info.year, info.month, info.day
               )
        # print(query)
        cursor = connection.cursor()
        cursor.execute(query)
        lastrowid = cursor.lastrowid
        print(f"inserted for {info.locdate} {lastrowid}")
        connection.commit()
    finally:
        connection.close()
        print("connection closed.")


def update(info):
    connection = sqlite3.connect(_DB)
    try:
        query = """
        INSERT INTO %s (
        ___sol_year, ___sol_month,
        __locdate, __date_kind, __date_name, __seq, __is_holiday, __remarks,
         __kst, __sun_longitude,
        _year, _month, _day
        ) VALUES (
        '%s', '%s',
        '%s', '%s', '%s', %d, '%s', '%s',
        '%s', '%s',
        %d, %d, %d
        )
        """ % (_TABLE,
               info.sol_year, info.date_kind,
               info.locdate, info.date_kind, info.date_name, info.seq, info.is_holiday, info.remarks,
               info.kst, info.sun_longitude,
               info.year, info.month, info.day
               )
        # print(query)
        cursor = connection.cursor()
        cursor.execute(query)
        lastrowid = cursor.lastrowid
        print(f"inserted for {info.locdate} {lastrowid}")
        connection.commit()
    finally:
        connection.close()
        print("connection closed.")

def main():
    ___sol_year = start_year
    while ___sol_year <= end_year:
        delete(___sol_year, '01')
        get_holi_de_info(___sol_year)
        get_rest_de_info(___sol_year)
        ___sol_year += 1


if __name__ == "__main__":
    main()