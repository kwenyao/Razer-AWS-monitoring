__author__ = 'Koh Wen Yao'

import boto.ec2 as ec2
import boto.ec2.cloudwatch as cwatch
import datetime
import time
import constants as c
import mysql.connector
import mysql_statements as s
import shared_functions as func


def initialiseConnections():
    ec2connection = ec2.connect_to_region(c.EC2_REGION,
                                          aws_access_key_id=c.ACCESS_KEY_ID,
                                          aws_secret_access_key=c.SECRET_ACCESS_KEY)
    cwConnection = cwatch.connect_to_region(c.EC2_REGION,
                                            aws_access_key_id=c.ACCESS_KEY_ID,
                                            aws_secret_access_key=c.SECRET_ACCESS_KEY)
    mysqlConnection = func.connectToMySQLServer()
    return ec2connection, cwConnection, mysqlConnection


def getAllMetrics(instances, connection):
    metrics = []
    for instance in instances:
        metric = connection.list_metrics(namespace="AWS/EC2", dimensions={'InstanceId': [instance]})
        metrics.extend(metric)
    return metrics


def getDataPoints(metric, start, end):
    unit = c.EC2_METRIC_UNIT_DICTIONARY.get(str(metric))
    if unit is None:
        return None
    else:
        return metric.query(start, end, c.EC2_METRIC_STAT_TYPE, unit, period=60)


def insertDataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor):
    instanceId, metricString, virtType, instanceType, keyName, amiId = metricTuple
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = str(datapoint.get(c.EC2_METRIC_STAT_TYPE))
        unit = datapoint.get('Unit')
        for group in securityGroups:
            securityGroup = str(group)[14:]
            # REMEMBER TO CHECK ORDER AFTER INSERTING NEW COLUMNS
            data = (amiId, instanceId, instanceType, keyName, metricString,
                    securityGroup, timestamp, unit, value, virtType)
            try:
                mysqlCursor.execute(s.ADD_EC2_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                print("MYSQL Error: {}".format(err))
    return


def insertMetricsToDB(metrics, securityGrpDict, instanceInfo, mysqlConn):
    mysqlCursor = mysqlConn.cursor()
    func.createTable(mysqlCursor)
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    for metric in metrics:
        metricString = str(metric)[7:]
        datapoints = getDataPoints(metric, start, end)
        instanceId = metric.dimensions.get('InstanceId')[0].replace('-', '_')
        securityGroups = securityGrpDict.get(instanceId)
        metricTuple = (instanceId, metricString) + instanceInfo.get(instanceId)
        if datapoints is None:
            continue
        else:
            insertDataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor)
    return


def buildSecurityGrpDictionary(ec2connection):
    securityGroups = ec2connection.get_all_security_groups()
    securityGrpDict = {}
    for group in securityGroups:
        instances = group.instances()
        if instances:
            for instance in instances:
                instanceId = str(instance)[9:].replace('-', '_')
                securityGrpList = securityGrpDict.get(instanceId)
                if securityGrpList is not None:
                    securityGrpList.append(group)
                    securityGrpDict[instanceId] = securityGrpList
                else:
                    securityGrpList = [group]
                    securityGrpDict[instanceId] = securityGrpList
    return securityGrpDict


def extractInstance(reservationList):
    instanceIds = []
    instanceInfo = {}
    for reservation in reservationList:
        instance = reservation.instances[0]
        instanceTuple = (instance.virtualization_type, instance.instance_type,
                         instance.key_name, instance.image_id.replace('-', '_'))
        instanceInfo[instance.id.replace('-', '_')] = instanceTuple
        instanceIds.append(instance.id)
    return instanceIds, instanceInfo


if __name__ == "__main__":
    startTime = time.time()
    ec2Conn, cwConn, mysqlConn = initialiseConnections()
    securityGrpDictionary = buildSecurityGrpDictionary(ec2Conn)
    reservationList = ec2Conn.get_all_instances()
    instanceIds, instanceInfo = extractInstance(reservationList)
    metrics = getAllMetrics(instanceIds, cwConn)
    insertMetricsToDB(metrics, securityGrpDictionary, instanceInfo, mysqlConn)
    mysqlConn.commit()
    mysqlConn.close()
    print "Execution Time: " + str(time.time() - startTime)
