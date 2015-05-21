__author__ = 'Koh Wen Yao'

import boto.ec2 as ec2
import boto.ec2.cloudwatch as cwatch
import boto.ec2.elb as elb
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
    elbconnection = elb.connect_to_region(c.ELB_REGION,
                                          aws_access_key_id=c.ACCESS_KEY_ID,
                                          aws_secret_access_key=c.SECRET_ACCESS_KEY)
    cwConnection = cwatch.connect_to_region(c.EC2_REGION,
                                            aws_access_key_id=c.ACCESS_KEY_ID,
                                            aws_secret_access_key=c.SECRET_ACCESS_KEY)

    # ec2connection = ec2.connect_to_region(region_name=c.EC2_REGION)
    # elbconnection = elb.connect_to_region(region_name=c.ELB_REGION)
    # cwConnection = cwatch.connect_to_region(region_name=c.EC2_REGION)

    mysqlConnection = func.connectToMySQLServer()
    return ec2connection, elbconnection, cwConnection, mysqlConnection

def getMetricDictionary(service):
    if service == c.SERVICE_TYPE_EC2:
        return c.EC2_METRIC_UNIT_DICTIONARY
    elif service == c.SERVICE_TYPE_ELB:
        return c.ELB_METRIC_UNIT_DICTIONARY

def getAllMetrics(connection, service):
    metricsList = []
    namespace = c.NAMESPACE_DICTIONARY.get(service)
    dictionary = getMetricDictionary(service)
    for metricName, (unit, statType) in dictionary.iteritems():
        if unit is not None:
            metrics = connection.list_metrics(namespace=namespace, metric_name=metricName)
            nextToken = metrics.next_token
            metricsList.extend(metrics)
            while nextToken:
                metrics = connection.list_metrics(namespace=namespace, metric_name=metricName, next_token=nextToken)
                nextToken = metrics.next_token
                metricsList.extend(metrics)
    return metricsList


def getDataPoints(metric, start, end, service):
    unitDict = getMetricDictionary(service)
    unit, statType = unitDict.get(str(metric)[7:])
    if unit is None:
        return None
    else:
        return metric.query(start, end, statType, unit, period=60)


def insertEC2DataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor):
    instanceId, metricString, virtType, instanceType, keyName, amiId = metricTuple
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = str(datapoint.get(c.EC2_METRIC_STAT_TYPE))
        unit = datapoint.get('Unit')
        for group in securityGroups:
            securityGroup = str(group)[14:]
            # REMEMBER TO CHECK ORDER AFTER INSERTING NEW COLUMNS
            data = (c.ACCOUNT_NAME, amiId, instanceId, instanceType, keyName, metricString, c.EC2_REGION,
                    securityGroup, c.SERVICE_TYPE_EC2, timestamp, unit, value, virtType)
            try:
                mysqlCursor.execute(s.ADD_EC2_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                # continue
                print("MYSQL Error: {}".format(err))
    return


def insertELBDatapointsToDB(loadBalancerName, datapoints, metric, mysqlCursor):
    unit, statType = c.ELB_METRIC_UNIT_DICTIONARY.get(metric)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = datapoint.get(statType)
        unit = datapoint.get('Unit')
        data = (c.ACCOUNT_NAME, loadBalancerName, metric, c.ELB_REGION,
                c.SERVICE_TYPE_ELB, timestamp, unit, value)
        try:
            mysqlCursor.execute(s.ADD_ELB_DATAPOINTS(), data)
        except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))


def insertEC2MetricsToDB(metrics, securityGrpDict, instanceInfo, mysqlConn):
    mysqlCursor = mysqlConn.cursor()
    func.createEC2Table(mysqlCursor)
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    for metric in metrics:
        datapoints = getDataPoints(metric, start, end, c.SERVICE_TYPE_EC2)
        if datapoints is None:
            continue
        else:
            if metric.dimensions.get('InstanceId') is None:
                continue
            instanceId = metric.dimensions.get('InstanceId')[0].replace('-', '_')
            if instanceInfo.get(instanceId) is None:
                continue
            else:
                securityGroups = securityGrpDict.get(instanceId)
                metricString = str(metric)[7:]
                metricTuple = (instanceId, metricString) + instanceInfo.get(instanceId)
                insertEC2DataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor)
    return


def insertELBMetricsToDB(metrics, mysqlConn):
    mysqlCursor = mysqlConn.cursor()
    func.createELBTable(mysqlCursor)
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    for metric in metrics:
        datapoints = getDataPoints(metric, start, end, c.SERVICE_TYPE_ELB)
        if metric.dimensions.get('LoadBalancerName') is None:
            continue
        loadBalancerName = metric.dimensions.get('LoadBalancerName')[0].replace('-', '_')
        if datapoints is None:
            continue
        else:
            insertELBDatapointsToDB(loadBalancerName, datapoints, str(metric)[7:], mysqlCursor)


def buildSecurityGrpDictionary(ec2connection):
    securityGroups = ec2connection.get_all_security_groups()
    securityGrpDict = {}
    for group in securityGroups:
        instances = group.instances()
        if instances:
            for instance in instances:
                if instance.state == 'running':
                    instanceId = str(instance)[9:].replace('-', '_')
                    securityGrpList = securityGrpDict.get(instanceId)
                    if securityGrpList is None:
                        securityGrpList = [group]
                        securityGrpDict[instanceId] = securityGrpList
                    else:
                        securityGrpList.append(group)
                        securityGrpDict[instanceId] = securityGrpList
    return securityGrpDict


def extractInstance(connection):
    filter = {'instance-state-name': 'running'}
    reservationList = connection.get_all_instances(filters=filter)
    instanceInfo = {}
    instanceList = []
    for reservation in reservationList:
        instance = reservation.instances[0]
        instanceTuple = (instance.virtualization_type, instance.instance_type,
                         instance.key_name, instance.image_id.replace('-', '_'))
        instanceInfo[instance.id.replace('-', '_')] = instanceTuple
        instanceList.append(instance.id)
    return instanceList, instanceInfo


def addAllEC2Datapoints(ec2Conn, cwConn, mysqlConn):
    securityGrpDictionary = buildSecurityGrpDictionary(ec2Conn)
    instanceList, instanceInfo = extractInstance(ec2Conn)
    metrics = getAllMetrics(cwConn, c.SERVICE_TYPE_EC2)
    insertEC2MetricsToDB(metrics, securityGrpDictionary, instanceInfo, mysqlConn)

def addAllELBDatapoints(elbConn, cwConn, mysqlConn):
    metrics = getAllMetrics(cwConn, c.SERVICE_TYPE_ELB)
    insertELBMetricsToDB(metrics, mysqlConn)

def execute():
    ec2Conn, elbConn, cwConn, mysqlConn = initialiseConnections()
    ec2Start = time.time()
    addAllEC2Datapoints(ec2Conn, cwConn, mysqlConn)
    ec2End = time.time()
    print "Time taken to add EC2 Datapoints: " + str(ec2End - ec2Start)
    addAllELBDatapoints(elbConn, cwConn, mysqlConn)
    print "Time taken to add ELB Datapoints: " + str(time.time() - ec2End)
    mysqlConn.commit()
    mysqlConn.close()


if __name__ == "__main__":
    startTime = time.time()
    execute()
    print "Execution Time: " + str(time.time() - startTime)
