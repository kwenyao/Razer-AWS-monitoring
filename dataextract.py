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
    elbconnection = elb.connect_to_region(c.EC2_REGION,
                                          aws_access_key_id=c.ACCESS_KEY_ID,
                                          aws_secret_access_key=c.SECRET_ACCESS_KEY)
    cwConnection = cwatch.connect_to_region(c.EC2_REGION,
                                            aws_access_key_id=c.ACCESS_KEY_ID,
                                            aws_secret_access_key=c.SECRET_ACCESS_KEY)

    loadBalancers = elbconnection.get_all_load_balancers()

    # metricELB = cwConnection.list_metrics(namespace='AWS/ELB')
    # print metricELB
    # ec2connection = ec2.connect_to_region(region_name=c.EC2_REGION)
    # cwConnection = cwatch.connect_to_region(region_name=c.EC2_REGION)

    mysqlConnection = func.connectToMySQLServer()
    return ec2connection, elbconnection, cwConnection, mysqlConnection


def getAllMetrics(instances, connection):
    metricsList = []
    for metricName, unit in c.EC2_METRIC_UNIT_DICTIONARY.iteritems():
        if unit is not None:
            metrics = connection.list_metrics(namespace="AWS/EC2", metric_name=metricName)
            nextToken = metrics.next_token
            metricsList = metrics
            while nextToken:
                metrics = connection.list_metrics(namespace="AWS/EC2", metric_name=metricName, next_token=nextToken)
                nextToken = metrics.next_token
                metricsList.extend(metrics)
    return metricsList


def getDataPoints(metric, start, end):
    unit = c.EC2_METRIC_UNIT_DICTIONARY.get(str(metric)[7:])
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
            data = (c.ACCOUNT_NAME, amiId, instanceId, instanceType, keyName, metricString, c.EC2_REGION,
                    securityGroup, c.EC2_SERVICE_TYPE, timestamp, unit, value, virtType)
            try:
                mysqlCursor.execute(s.ADD_EC2_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                # continue
                print("MYSQL Error: {}".format(err))
    return


def insertMetricsToDB(metrics, securityGrpDict, instanceInfo, mysqlConn):
    mysqlCursor = mysqlConn.cursor()
    func.createTable(mysqlCursor)
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    for metric in metrics:
        datapoints = getDataPoints(metric, start, end)
        if datapoints is None:
            continue
        else:
            print metric
            if metric.dimensions.get('InstanceId') is None:
                continue
            instanceId = metric.dimensions.get('InstanceId')[0].replace('-', '_')
            if instanceInfo.get(instanceId) is None:
                continue
            else:
                securityGroups = securityGrpDict.get(instanceId)
                metricString = str(metric)[7:]
                metricTuple = (instanceId, metricString) + instanceInfo.get(instanceId)
                insertDataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor)
    return


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
                    if securityGrpList is not None:
                        securityGrpList.append(group)
                        securityGrpDict[instanceId] = securityGrpList
                    else:
                        securityGrpList = [group]
                        securityGrpDict[instanceId] = securityGrpList
    return securityGrpDict


def extractInstance(connection):
    filter = {'instance-state-name': 'running'}
    reservationList = connection.get_all_instances(filters=filter)
    instanceIds = []
    instanceInfo = {}
    for reservation in reservationList:
        print reservation.instances
        instance = reservation.instances[0]
        instanceTuple = (instance.virtualization_type, instance.instance_type,
                         instance.key_name, instance.image_id.replace('-', '_'))
        instanceInfo[instance.id.replace('-', '_')] = instanceTuple
        instanceIds.append(instance.id)
    return instanceIds, instanceInfo


def execute():
    ec2Conn, cwConn, mysqlConn = initialiseConnections()
    securityGrpDictionary = buildSecurityGrpDictionary(ec2Conn)
    instanceIds, instanceInfo = extractInstance(ec2Conn)
    metrics = getAllMetrics(instanceIds, cwConn)
    insertMetricsToDB(metrics, securityGrpDictionary, instanceInfo, mysqlConn)
    mysqlConn.commit()
    mysqlConn.close()

if __name__ == "__main__":
    startTime = time.time()
    execute()
    print "Execution Time: " + str(time.time() - startTime)
