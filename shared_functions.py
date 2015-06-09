__author__ = 'Koh Wen Yao'

import mysql.connector
import MySQLdb
import constants as c
import mysql_statements as s
from warnings import filterwarnings


def connect_to_mysql_server():
    create_database()
    connection = mysql.connector.connect(user=c.MYSQL_USER,
                                         password=c.MYSQL_PASSWORD,
                                         host=c.MYSQL_HOST,
                                         database=c.DATABASE_NAME)
    return connection


def convert_datetime_to_string(datetime_var):
    datetime_string = datetime_var.strftime("%Y/%m/%d %H:%M:%S")
    return datetime_string


def create_database():
    filterwarnings('ignore', category=MySQLdb.Warning)
    connection = MySQLdb.connect(host=c.MYSQL_HOST, user=c.MYSQL_USER, passwd=c.MYSQL_PASSWORD)
    cursor = connection.cursor()
    try:
        cursor.execute(s.CREATE_DATABASE)
    except MySQLdb.Error as err:
        print("MYSQL Error: {}".format(err))
    cursor.close()
    connection.close()


def create_ec2_table(cursor):
    try:
        cursor.execute(s.CREATE_EC2_TABLE())
    except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))


def create_elb_table(cursor):
    try:
        cursor.execute(s.CREATE_ELB_TABLE())
    except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))


def create_rds_table(cursor):
    try:
        cursor.execute(s.CREATE_RDS_TABLE())
    except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))


def exists(it):
    return it is not None
