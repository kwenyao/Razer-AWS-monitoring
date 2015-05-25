__author__ = 'Koh Wen Yao'

import boto.ec2 as ec2
import boto.ec2.cloudwatch as cwatch
import boto.rds as rds
import datetime
import time
import constants as c
import mysql.connector
import mysql_statements as s
import shared_functions as func
import multiprocessing
import multiprocessing.pool

class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


def initialiseConnections(region):
    # FOR TESTING ON REMOTE MACHINE
    # ec2connection = ec2.connect_to_region(region_name=region,
    #                                       aws_access_key_id=c.ACCESS_KEY_ID,
    #                                       aws_secret_access_key=c.SECRET_ACCESS_KEY)
    # rdsconnection = rds.connect_to_region(region_name=region,
    #                                       aws_access_key_id=c.ACCESS_KEY_ID,
    #                                       aws_secret_access_key=c.SECRET_ACCESS_KEY)
    # cwConnection = cwatch.connect_to_region(region_name=region,
    #                                         aws_access_key_id=c.ACCESS_KEY_ID,
    #                                         aws_secret_access_key=c.SECRET_ACCESS_KEY)

    ec2connection = ec2.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)
    rdsconnection = rds.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)
    cwConnection = cwatch.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)

    mysqlConnection = func.connectToMySQLServer()
    return ec2connection, rdsconnection, cwConnection, mysqlConnection

def getMetricDictionary(service):
    if service == c.SERVICE_TYPE_EC2:
        return c.EC2_METRIC_UNIT_DICTIONARY
    elif service == c.SERVICE_TYPE_ELB:
        return c.ELB_METRIC_UNIT_DICTIONARY
    elif service == c.SERVICE_TYPE_RDS:
        return c.RDS_METRIC_UNIT_DICTIONARY

def filterMetricList(metricsList):
    filteredMetricList = []
    for metric in metricsList:
        if not metric.dimensions.get("AvailabilityZone"):
            filteredMetricList.append(metric)
    return filteredMetricList

def getAllMetrics(connection, service):
    metricsList = []
    namespace = c.NAMESPACE_DICTIONARY.get(service)
    dictionary = getMetricDictionary(service)
    for metricName, (unit, statType) in dictionary.iteritems():
        if unit is not None:
            metrics = connection.list_metrics(namespace=namespace,
                                              metric_name=metricName)
            nextToken = metrics.next_token
            metricsList.extend(metrics)
            while nextToken:
                metrics = connection.list_metrics(namespace=namespace,
                                                  metric_name=metricName,
                                                  next_token=nextToken)
                nextToken = metrics.next_token
                metricsList.extend(metrics)
    return filterMetricList(metricsList)


def queryDataPoints(metric, service):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    unitDict = getMetricDictionary(service)
    unit, statType = unitDict.get(str(metric)[7:])
    if unit is None:
        return None
    else:
        return metric.query(start, end, statType, unit, period=60)

def ec2pool(metric):
    if metric.dimensions.get('InstanceId') is None:
        return
    datapoints = queryDataPoints(metric, c.SERVICE_TYPE_EC2)
    if not datapoints:
        return
    else:
        return metric, datapoints


def elbpool(metric):
    if metric.dimensions.get('LoadBalancerName') is None:
        return
    datapoints = queryDataPoints(metric, c.SERVICE_TYPE_ELB)
    if not datapoints:
        return
    else:
        return metric, datapoints


def rdspool(metric):
    if metric.dimensions.get('DBInstanceIdentifier') is None:
        return
    datapoints = queryDataPoints(metric, c.SERVICE_TYPE_RDS)
    if not datapoints:
        return
    else:
        return metric, datapoints


def insertEC2DataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor, region):
    instanceId, metricString, virtType, instanceType, keyName, amiId = metricTuple
    unit, statType = c.EC2_METRIC_UNIT_DICTIONARY.get(metricString)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = str(datapoint.get(statType))
        unit = datapoint.get('Unit')
        for group in securityGroups:
            securityGroup = str(group)[14:]
            # REMEMBER TO CHECK ORDER AFTER INSERTING NEW COLUMNS
            data = (c.ACCOUNT_NAME, amiId, instanceId, instanceType, keyName, metricString, region,
                    securityGroup, c.SERVICE_TYPE_EC2, timestamp, unit, value, virtType)
            try:
                mysqlCursor.execute(s.ADD_EC2_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                # continue
                print("MYSQL Error: {}".format(err))
    return


def insertELBDatapointsToDB(loadBalancerName, datapoints, metric, mysqlCursor, region):
    unit, statType = c.ELB_METRIC_UNIT_DICTIONARY.get(metric)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = datapoint.get(statType)
        unit = datapoint.get('Unit')
        data = (c.ACCOUNT_NAME, loadBalancerName, metric, region,
                c.SERVICE_TYPE_ELB, timestamp, unit, value)
        try:
            mysqlCursor.execute(s.ADD_ELB_DATAPOINTS(), data)
        except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))
    return


def insertRDSDatapointsToDB(instanceName, instanceInfo, datapoints, metric, mysqlCursor, region):
    unit, statType = c.RDS_METRIC_UNIT_DICTIONARY.get(metric)
    multiAZ = instanceInfo.get(c.COLUMN_NAME_RDS_MULTI_AZ)
    securityGroups = instanceInfo.get(c.COLUMN_NAME_RDS_SECURITY_GRP)
    engine = instanceInfo.get(c.COLUMN_NAME_RDS_ENGINE)
    instanceClass = instanceInfo.get(c.COLUMN_NAME_RDS_INSTANCE_CLASS)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = datapoint.get(statType)
        unit = datapoint.get('Unit')
        for securityGroup in securityGroups:
            securityGroupString = str(securityGroup)
            data = (c.ACCOUNT_NAME, engine, instanceClass, metric, multiAZ,
                    instanceName, region, securityGroupString, timestamp, unit, value)
            try:
                mysqlCursor.execute(s.ADD_RDS_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                print("MYSQL Error: {}".format(err))
    return


def getAllDataPoints(metrics, service):
    if service == c.SERVICE_TYPE_EC2:
        function = ec2pool
    elif service == c.SERVICE_TYPE_ELB:
        function = elbpool
    elif service == c.SERVICE_TYPE_RDS:
        function = rdspool
    else:
        return
    pool = multiprocessing.Pool(c.POOL_SIZE)
    datalist = pool.map(function, metrics)
    filteredList = filter(func.exists, datalist)
    return filteredList


def insertEC2MetricsToDB(metrics, securityGrpDict, instanceInfo, mysqlConn, region):
    mysqlCursor = mysqlConn.cursor()
    func.createEC2Table(mysqlCursor)
    dataList = getAllDataPoints(metrics, c.SERVICE_TYPE_EC2)
    for metric, datapoints in dataList:
        instanceId = metric.dimensions.get('InstanceId')[0].replace('-', '_')
        if instanceInfo.get(instanceId) is None:
            continue
        else:
            securityGroups = securityGrpDict.get(instanceId)
            metricString = str(metric)[7:]
            metricTuple = (instanceId, metricString) + instanceInfo.get(instanceId)
            insertEC2DataPointsToDB(datapoints, securityGroups, metricTuple, mysqlCursor, region)
    return


def insertELBMetricsToDB(metrics, mysqlConn, region):
    mysqlCursor = mysqlConn.cursor()
    func.createELBTable(mysqlCursor)
    dataList = getAllDataPoints(metrics, c.SERVICE_TYPE_ELB)
    for metric, datapoints in dataList:
        loadBalancerName = metric.dimensions.get('LoadBalancerName')[0].replace('-', '_')
        insertELBDatapointsToDB(loadBalancerName, datapoints, str(metric)[7:], mysqlCursor, region)
    return


def insertRDSMetricsToDB(metrics, instanceDict, mysqlConn, region):
    mysqlCursor = mysqlConn.cursor()
    func.createRDSTable(mysqlCursor)
    dataList = getAllDataPoints(metrics, c.SERVICE_TYPE_RDS)
    for metric, datapoints in dataList:
        instanceName = metric.dimensions.get('DBInstanceIdentifier')[0].replace('-', '_')
        if datapoints is None:
            continue
        else:
            instanceInfo = instanceDict.get(instanceName)
            insertRDSDatapointsToDB(instanceName, instanceInfo, datapoints, str(metric)[7:], mysqlCursor, region)
    return


def buildEC2SecurityGrpDictionary(ec2connection):
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

def extractEC2Instances(connection):
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


def extractDBInstances(rdsConn):
    instances = rdsConn.get_all_dbinstances()
    instanceDict = {}
    for instance in instances:
        instanceInfo = {}
        instanceInfo[c.COLUMN_NAME_RDS_MULTI_AZ] = instance.multi_az
        instanceInfo[c.COLUMN_NAME_RDS_SECURITY_GRP] = instance.security_groups
        instanceInfo[c.COLUMN_NAME_RDS_ENGINE] = instance.engine
        instanceInfo[c.COLUMN_NAME_RDS_INSTANCE_CLASS] = instance.instance_class
        instanceDict[instance.id] = instanceInfo
    return instanceDict


def addAllEC2Datapoints(ec2Conn, cwConn, mysqlConn, region):
    securityGrpDictionary = buildEC2SecurityGrpDictionary(ec2Conn)
    instanceList, instanceInfo = extractEC2Instances(ec2Conn)
    metrics = getAllMetrics(cwConn, c.SERVICE_TYPE_EC2)
    insertEC2MetricsToDB(metrics, securityGrpDictionary, instanceInfo, mysqlConn, region)
    return


def addAllELBDatapoints(cwConn, mysqlConn, region):
    metrics = getAllMetrics(cwConn, c.SERVICE_TYPE_ELB)
    insertELBMetricsToDB(metrics, mysqlConn, region)
    return


def addAllRDSDatapoints(rdsConn, cwConn, mysqlConn, region):
    metrics = getAllMetrics(cwConn, c.SERVICE_TYPE_RDS)
    instanceDict = extractDBInstances(rdsConn)
    insertRDSMetricsToDB(metrics, instanceDict, mysqlConn, region)
    return

def execute(region):
    startTime = time.time()
    regionString = region.replace("-", "_")
    ec2Conn, rdsConn, cwConn, mysqlConn = initialiseConnections(region)
    ec2Start = time.time()
    addAllEC2Datapoints(ec2Conn, cwConn, mysqlConn, regionString)
    ec2End = time.time()
    print "Time taken to add EC2 Datapoints (" + c.ACCOUNT_NAME + region + "): " + str(ec2End - ec2Start)
    addAllELBDatapoints(cwConn, mysqlConn, regionString)
    elbEnd = time.time()
    print "Time taken to add ELB Datapoints: (" + c.ACCOUNT_NAME + region + "): " + str(elbEnd - ec2End)
    addAllRDSDatapoints(rdsConn, cwConn, mysqlConn, regionString)
    print "Time taken to add RDS Datapoints: (" + c.ACCOUNT_NAME + region + "): " + str(time.time() - elbEnd)
    mysqlConn.commit()
    mysqlConn.close()
    print "Execution Time: (" + c.ACCOUNT_NAME + region + "): " + str(time.time() - startTime)


if __name__ == "__main__":
    pool = MyPool(c.REGION_POOL_SIZE)
    pool.map(execute, c.REGION_LIST)
