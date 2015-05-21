__author__ = 'Koh Wen Yao'

import constants as c
import mysql_statements as s
import shared_functions as func
import datetime
import time
import mysql.connector
from elasticsearch import Elasticsearch

def buildBulkBody(cursor):
    if cursor is None:
        return None
    else:
        columns = tuple([d[0].decode('utf8') for d in cursor.description])
        dictionary = {}
        queryResult = []
        for row in cursor:
            dictionary['index'] = {}
            queryResult.append(dictionary)
            queryResult.append(dict(zip(columns, row)))
        return queryResult


def pushDataToElasticsearch(queryResult, nowTime, service):
    es = Elasticsearch([c.ELASTICSEARCH_URL])
    index = 'monitoring'
    type = service.lower()
    response = es.bulk(index=index, doc_type=type, body=queryResult)
    print "Errors: " + str(response.get('errors'))
    es.indices.refresh(index=index)


def getDataFromDB(cursor):
    nowTime = datetime.datetime.utcnow()
    startTime = nowTime - datetime.timedelta(minutes=c.DATA_RETRIEVAL_TIME_DELTA_MINUTES)
    startTimeString = func.convertDateTimeToString(startTime)
    for service, query in s.DATA_RETRIEVAL_DICTIONARY.iteritems():
        print "pushing " + service + " data into elasticsearch"
        try:
            cursor.execute(query, (startTimeString,))
        except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))
        queryResult = buildBulkBody(cursor)
        if queryResult:
            pushDataToElasticsearch(queryResult, nowTime, service)


if __name__ == "__main__":
    startTime = time.time()
    mysqlConnection = func.connectToMySQLServer()
    mysqlCursor = mysqlConnection.cursor()
    getDataFromDB(mysqlCursor)
    mysqlCursor.close()
    mysqlConnection.close()
    print "Total Execution Time: " + str(time.time() - startTime)
