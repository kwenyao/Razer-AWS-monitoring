__author__ = 'Koh Wen Yao'

import constants as c

FIND_EC2_METRICS = ("SELECT * FROM {0} WHERE {1} >= %s".format(c.TABLE_NAME_EC2, c.COLUMN_NAME_EC2_TIMESTAMP))

FIND_ELB_METRICS = ("SELECT * FROM {0} WHERE {1} >= %s".format(c.TABLE_NAME_ELB, c.COLUMN_NAME_ELB_TIMESTAMP))

FIND_RDS_METRICS = ("SELECT * FROM {0} WHERE {1} >= %s".format(c.TABLE_NAME_RDS, c.COLUMN_NAME_RDS_TIMESTAMP))

FIND_ALL_EC2 = ("SELECT * FROM {0}".format(c.TABLE_NAME_EC2))

FIND_ALL_ELB = ("SELECT * FROM {0}".format(c.TABLE_NAME_ELB))

FIND_ALL_RDS = ("SELECT * FROM {0}".format(c.TABLE_NAME_RDS))

FIND_ALL_COLUMNS = ("SHOW COLUMNS FROM {0}".format(c.TABLE_NAME_EC2))

CREATE_DATABASE = ("CREATE DATABASE IF NOT EXISTS {0}".format(c.DATABASE_NAME))

DATA_RETRIEVAL_DICTIONARY = {c.SERVICE_TYPE_EC2: FIND_EC2_METRICS,
                             c.SERVICE_TYPE_ELB: FIND_ELB_METRICS,
                             c.SERVICE_TYPE_RDS: FIND_RDS_METRICS}

ALL_DATA_RETRIEVAL_DICTIONARY = {c.SERVICE_TYPE_EC2: FIND_ALL_EC2,
                                 c.SERVICE_TYPE_ELB: FIND_ALL_ELB,
                                 c.SERVICE_TYPE_RDS: FIND_ALL_RDS}


def build_primary_key(service):
    primary_keys = c.PRIMARY_KEY_DICTIONARY.get(service)
    string = ''
    for primaryKey in primary_keys:
        string += primaryKey
        string += ', '
    return string[:-2]


def CREATE_EC2_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS {0}(".format(c.TABLE_NAME_EC2)
    statement2 = ""
    for column, dataType in c.EC2_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    primary_key = build_primary_key(c.SERVICE_TYPE_EC2)
    statement3 = "PRIMARY KEY({0}))".format(primary_key)
    return statement1 + statement2 + statement3


def CREATE_ELB_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS {0}(".format(c.TABLE_NAME_ELB)
    statement2 = ""
    for column, dataType in c.ELB_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    primary_key = build_primary_key(c.SERVICE_TYPE_ELB)
    statement3 = "PRIMARY KEY({0}))".format(primary_key)
    return statement1 + statement2 + statement3


def CREATE_RDS_TABLE():
    statement1 = "CREATE TABLE IF NOT EXISTS " + c.TABLE_NAME_RDS + "("
    statement2 = ""
    for column, dataType in c.RDS_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column
        statement2 += " "
        statement2 += dataType + ","
    primary_key = build_primary_key(c.SERVICE_TYPE_RDS)
    statement3 = "PRIMARY KEY({0}))".format(primary_key)
    return statement1 + statement2 + statement3


def ADD_EC2_DATAPOINTS():
    statement1 = "REPLACE INTO {0} (".format(c.TABLE_NAME_EC2)
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
    statement1 = "REPLACE INTO " + c.TABLE_NAME_ELB + " ("
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


def ADD_RDS_DATAPOINTS():
    statement1 = "REPLACE INTO {0} (".format(c.TABLE_NAME_RDS)
    statement2 = ""
    for column, dataType in c.RDS_DATAPOINT_ATTR_TYPE_DICTIONARY.iteritems():
        statement2 += column + ", "
    statement2 = statement2[:-2]
    statement3 = ") VALUES ("
    for x in range(0, len(c.RDS_DATAPOINT_ATTR_TYPE_DICTIONARY)):
        statement3 += "%s, "
    statement3 = statement3[:-2]
    statement3 += ")"
    return statement1 + statement2 + statement3
