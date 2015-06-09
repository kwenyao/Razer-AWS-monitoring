__author__ = 'Koh Wen Yao'

import constants as c
import MySQLdb
import MySQLdb.cursors
import mysql_statements as s
import shared_functions as func
import datetime
import time
import sys
from elasticsearch import Elasticsearch


def create_id(result_dictionary, service):
    primary_keys = c.PRIMARY_KEY_DICTIONARY.get(service)
    id = ''
    for primaryKey in primary_keys:
        value = result_dictionary.get(primaryKey)
        if isinstance(value, str):
            id += value
        else:
            id += str(value)
    id = id.replace(' ', '').lower()
    return id


def build_bulk_body(rows, columns, service):
    query_result = []
    for row in rows:
        dictionary = {}
        result_dict = dict(zip(columns, row))
        id = create_id(result_dict, service)
        dictionary['index'] = {"_id": id}
        query_result.append(dictionary)
        query_result.append(result_dict)
    if query_result:
        push_data_to_elasticsearch(query_result, service)
    return


def push_data_to_elasticsearch(query_result, service):
    es = Elasticsearch([c.ELASTICSEARCH_URL])
    index = 'monitoring'
    type = service.lower()
    response = es.bulk(index=index, doc_type=type, body=query_result)
    print "Errors: {0}".format(str(response.get('errors')))
    es.indices.refresh(index=index)
    return


def execute_query(cursor, query, is_fetch_all):
    if is_fetch_all:
        try:
            cursor.execute(query)
        except MySQLdb.Error as err:
            print("MYSQL Error: {}".format(err))
    else:
        now_time = datetime.datetime.utcnow()
        start_time = now_time - datetime.timedelta(minutes=c.DATA_RETRIEVAL_TIME_DELTA_MINUTES)
        start_time_string = func.convert_datetime_to_string(start_time)
        try:
            cursor.execute(query, (start_time_string,))
        except MySQLdb.Error as err:
            print("MYSQL Error: {}".format(err))
    return cursor


def get_data_from_db(mysql_connection):
    is_fetch_all = False
    if len(sys.argv) > 1 and sys.argv[1] == 'all':
        dictionary = s.ALL_DATA_RETRIEVAL_DICTIONARY
        is_fetch_all = True
    else:
        dictionary = s.DATA_RETRIEVAL_DICTIONARY
    for service, query in dictionary.iteritems():
        cursor = mysql_connection.cursor()
        print "Pushing " + service + " data into elasticsearch"
        cursor = execute_query(cursor, query, is_fetch_all)
        columns = tuple([d[0].decode('utf8') for d in cursor.description])
        while True:
            rows = cursor.fetchmany(c.ROWS_TO_FETCH)
            if not rows:
                break
            else:
                build_bulk_body(rows, columns, service)
        cursor.close()
    return


if __name__ == "__main__":
    startTime = time.time()
    mysqlConnection = MySQLdb.connect(host=c.MYSQL_HOST, user=c.MYSQL_USER,
                                      passwd=c.MYSQL_PASSWORD, db=c.DATABASE_NAME,
                                      cursorclass=MySQLdb.cursors.SSCursor)
    get_data_from_db(mysqlConnection)
    mysqlConnection.close()
    print "Total Execution Time: {0}".format(str(time.time() - startTime))
