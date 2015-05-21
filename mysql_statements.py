__author__ = 'Koh Wen Yao'

import constants as c

FIND_ALL_EC2_METRICS = ("SELECT * FROM " + c.TABLE_NAME_EC2 +
                        " WHERE " + c.COLUMN_NAME_EC2_TIMESTAMP + " >= %s")

FIND_ALL_ELB_METRICS = ("SELECT * FROM " + c.TABLE_NAME_ELB +
                        " WHERE " + c.COLUMN_NAME_ELB_TIMESTAMP + " >= %s")

FIND_ALL_COLUMNS = ("SHOW COLUMNS FROM " + c.TABLE_NAME_EC2)

CREATE_DATABASE = ("CREATE DATABASE IF NOT EXISTS " + c.DATABASE_NAME)


def CREATE_EC2_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS " + c.TABLE_NAME_EC2 + "("
    statement2 = ""
    for column, dataType in c.EC2_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    statement3 = "PRIMARY KEY(" + c.PRIMARY_KEYS_EC2 + "))"
    return statement1 + statement2 + statement3

def CREATE_ELB_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS " + c.TABLE_NAME_ELB + "("
    statement2 = ""
    for column, dataType in c.ELB_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    statement3 = "PRIMARY KEY(" + c.PRIMARY_KEYS_ELB + "))"
    return statement1 + statement2 + statement3


def ADD_EC2_DATAPOINTS():
    statement1 = "INSERT INTO " + c.TABLE_NAME_EC2 + " ("
    statement2 = ""
    for column, dataType in c.EC2_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column + ", "
    statement2 = statement2[:-2]
    statement3 = ") VALUES ("
    for x in range(0, len(c.EC2_DATAPOINT_ATTR_TYPE_DICTIONARY)):
        statement3 += "%s, "
    statement3 = statement3[:-2]
    statement3 += ")"
    return statement1 + statement2 + statement3


def ADD_ELB_DATAPOINTS():
    statement1 = "INSERT INTO " + c.TABLE_NAME_ELB + " ("
    statement2 = ""
    for column, dataType in c.ELB_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column + ", "
    statement2 = statement2[:-2]
    statement3 = ") VALUES ("
    for x in range(0, len(c.ELB_DATAPOINT_ATTR_TYPE_DICTIONARY)):
        statement3 += "%s, "
    statement3 = statement3[:-2]
    statement3 += ")"
    return statement1 + statement2 + statement3