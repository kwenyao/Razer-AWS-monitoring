__author__ = 'Koh Wen Yao'

import mysql.connector
import MySQLdb
import constants as c
import mysql_statements as s
from warnings import filterwarnings


def connectToMySQLServer():
    createDatabase()
    connection = mysql.connector.connect(user=c.MYSQL_USER, password=c.MYSQL_PASSWORD,
                                         host=c.MYSQL_HOST,
                                         database=c.DATABASE_NAME)
    return connection


def convertDateTimeToString(datetimeVar):
    datetimeString = datetimeVar.strftime("%Y/%m/%d %H:%M:%S")
    return datetimeString


def createDatabase():
    filterwarnings('ignore', category=MySQLdb.Warning)
    connection = MySQLdb.connect(host=c.MYSQL_HOST, user=c.MYSQL_USER, passwd=c.MYSQL_PASSWORD)
    cursor = connection.cursor()
    try:
        cursor.execute(s.CREATE_DATABASE)
    except MySQLdb.Error as err:
        print("MYSQL Error: {}".format(err))
    cursor.close()
    connection.close()


def createTable(cursor):
    try:
        cursor.execute(s.CREATE_EC2_TABLE())
    except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))