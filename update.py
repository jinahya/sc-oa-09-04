#!/usr/bin/env python3
import sqlite3
try:
    connection = sqlite3.connect("./db/sc-oa-09-04.db")
finally:
    connection.close()
        
