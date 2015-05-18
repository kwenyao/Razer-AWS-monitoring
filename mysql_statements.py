__author__ = 'Koh Wen Yao'

import constants as c

FIND_ALL_EC2_METRICS = ("SELECT * FROM " + c.EC2_METRICS_TABLE_NAME +
                        " WHERE " + c.COLUMN_NAME_TIMESTAMP + " >= %s")

FIND_ALL_COLUMNS = ("SHOW COLUMNS FROM " + c.EC2_METRICS_TABLE_NAME)

CREATE_DATABASE = ("CREATE DATABASE IF NOT EXISTS " + c.DATABASE_NAME)


def CREATE_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS " + c.EC2_METRICS_TABLE_NAME + "("
    statement2 = ""
    for column, dataType in c.EC2_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    statement3 = "PRIMARY KEY(" + c.PRIMARY_KEYS + "))"
    return statement1 + statement2 + statement3


def ADD_EC2_DATAPOINTS():
    statement1 = "INSERT INTO " + c.EC2_METRICS_TABLE_NAME + " ("
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
