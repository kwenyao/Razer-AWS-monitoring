__author__ = 'Koh Wen Yao'

import constants as c
import mysql_statements as s
import shared_functions as func
import datetime
import time
import mysql.connector
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


def build_bulk_body(cursor, service):
    if cursor.description is None:
        return None
    else:
        columns = tuple([d[0].decode('utf8') for d in cursor.description])
        query_result = []
        for row in cursor:
            dictionary = {}
            result_dict = dict(zip(columns, row))
            id = create_id(result_dict, service)
            dictionary['index'] = {"_id": id}
            query_result.append(dictionary)
            query_result.append(result_dict)
        return query_result


def push_data_to_elasticsearch(query_result, service):
    es = Elasticsearch([c.ELASTICSEARCH_URL])
    index = 'monitoring'
    type = service.lower()
    response = es.bulk(index=index, doc_type=type, body=query_result)
    print "Errors: {0}".format(str(response.get('errors')))
    es.indices.refresh(index=index)


def get_data_from_db(cursor):
    now_time = datetime.datetime.utcnow()
    start_time = now_time - datetime.timedelta(minutes=c.DATA_RETRIEVAL_TIME_DELTA_MINUTES)
    start_time_string = func.convert_datetime_to_string(start_time)
    for service, query in s.DATA_RETRIEVAL_DICTIONARY.iteritems():
        print "pushing " + service + " data into elasticsearch"
        try:
            # cursor.execute(s.FIND_ALL) # FOR TESTING ONLY
            cursor.execute(query, (start_time_string,))
        except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))
        query_result = build_bulk_body(cursor, service)
        if query_result:
            push_data_to_elasticsearch(query_result, service)


if __name__ == "__main__":
    startTime = time.time()
    mysqlConnection = func.connect_to_mysql_server()
    mysqlCursor = mysqlConnection.cursor()
    get_data_from_db(mysqlCursor)
    mysqlCursor.close()
    mysqlConnection.close()
    print "Total Execution Time: {0}".format(str(time.time() - startTime))
