#!/usr/bin/env python3
import sqlite3
URL = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService'
try:
    connection = sqlite3.connect("./db/sc-oa-09-04.db")
finally:
    connection.close()
        
