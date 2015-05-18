__author__ = 'Koh Wen Yao'

import constants as c
import mysql_statements as s
import shared_functions as func
import datetime
import time
import mysql.connector
from elasticsearch import Elasticsearch


def getDataFromDB(cursor):
    nowTime = datetime.datetime.utcnow()
    startTime = nowTime - datetime.timedelta(minutes=c.DATA_RETRIEVAL_TIME_DELTA_MINUTES)
    startTimeString = func.convertDateTimeToString(startTime)
    es = Elasticsearch([c.ELASTICSEARCH_URL])
    try:
        cursor.execute(s.FIND_ALL_EC2_METRICS, (startTimeString,))
    except mysql.connector.Error as err:
        print("MYSQL Error: {}".format(err))
    queryResult = []
    columns = tuple([d[0].decode('utf8') for d in cursor.description])
    dictionary = {}
    index = 0
    for row in cursor:
        dictionary['index'] = {}
        queryResult.append(dictionary)
        queryResult.append(dict(zip(columns, row)))
        index += 1
    if queryResult:
        index = 'monitoring'
        type = func.convertDateTimeToString(nowTime)
        response = es.bulk(index=index, doc_type=type, body=queryResult)
        print response.get('created')
        es.indices.refresh(index=index)


if __name__ == "__main__":
    startTime = time.time()
    mysqlConnection = func.connectToMySQLServer()
    mysqlCursor = mysqlConnection.cursor()
    getDataFromDB(mysqlCursor)
    mysqlCursor.close()
    mysqlConnection.close()
    print "Total Execution Time: " + str(time.time() - startTime)

