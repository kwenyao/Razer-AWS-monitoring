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


class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


def initialise_connections(region):
    # FOR TESTING ON REMOTE MACHINE
    # ec2connection = ec2.connect_to_region(region_name=region,
    #                                       aws_access_key_id=c.ACCESS_KEY_ID,
    #                                       aws_secret_access_key=c.SECRET_ACCESS_KEY)
    # rds_connection = rds.connect_to_region(region_name=region,
    #                                       aws_access_key_id=c.ACCESS_KEY_ID,
    #                                       aws_secret_access_key=c.SECRET_ACCESS_KEY)
    # cw_connection = cwatch.connect_to_region(region_name=region,
    #                                         aws_access_key_id=c.ACCESS_KEY_ID,
    #                                         aws_secret_access_key=c.SECRET_ACCESS_KEY)
    ec2connection = ec2.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)
    rds_connection = rds.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)
    cw_connection = cwatch.connect_to_region(region_name=region, profile_name=c.PROFILE_NAME)
    mysql_connection = func.connect_to_mysql_server()
    return ec2connection, rds_connection, cw_connection, mysql_connection


def get_metric_dictionary(service):
    if service == c.SERVICE_TYPE_EC2:
        return c.EC2_METRIC_UNIT_DICTIONARY
    elif service == c.SERVICE_TYPE_ELB:
        return c.ELB_METRIC_UNIT_DICTIONARY
    elif service == c.SERVICE_TYPE_RDS:
        return c.RDS_METRIC_UNIT_DICTIONARY


def filter_metric_list(metrics_list):
    filtered_metric_list = []
    for metric in metrics_list:
        if not metric.dimensions.get("AvailabilityZone"):
            filtered_metric_list.append(metric)
    return filtered_metric_list


def get_all_metrics(connection, service):
    metrics_list = []
    namespace = c.NAMESPACE_DICTIONARY.get(service)
    dictionary = get_metric_dictionary(service)
    for metricName, (unit, statType) in dictionary.iteritems():
        if unit is not None:
            metrics = connection.list_metrics(namespace=namespace,
                                              metric_name=metricName)
            next_token = metrics.next_token
            metrics_list.extend(metrics)
            while next_token:
                metrics = connection.list_metrics(namespace=namespace,
                                                  metric_name=metricName,
                                                  next_token=next_token)
                next_token = metrics.next_token
                metrics_list.extend(metrics)
    return filter_metric_list(metrics_list)


def query_data_points(metric, service):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=c.MONITORING_TIME_MINUTES)
    unit_dict = get_metric_dictionary(service)
    unit, stat_type = unit_dict.get(str(metric)[7:])
    if unit is None:
        return None
    else:
        return metric.query(start, end, stat_type, unit, period=60)


def ec2pool(metric):
    if metric.dimensions.get('InstanceId') is None:
        return
    datapoints = query_data_points(metric, c.SERVICE_TYPE_EC2)
    if not datapoints:
        return
    else:
        return metric, datapoints


def elbpool(metric):
    if metric.dimensions.get('LoadBalancerName') is None:
        return
    datapoints = query_data_points(metric, c.SERVICE_TYPE_ELB)
    if not datapoints:
        return
    else:
        return metric, datapoints


def rdspool(metric):
    if metric.dimensions.get('DBInstanceIdentifier') is None:
        return
    datapoints = query_data_points(metric, c.SERVICE_TYPE_RDS)
    if not datapoints:
        return
    else:
        return metric, datapoints


def insert_ec2_datapoints_to_db(datapoints, security_groups, metric_tuple, mysql_cursor, region):
    instance_id, metric_string, virt_type, instance_type, key_name, ami_id = metric_tuple
    unit, stat_type = c.EC2_METRIC_UNIT_DICTIONARY.get(metric_string)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = str(datapoint.get(stat_type))
        unit = datapoint.get('Unit')
        for group in security_groups:
            security_group = str(group)[14:]
            data = (c.ACCOUNT_NAME, ami_id, instance_id, instance_type, key_name, metric_string, region,
                    security_group, c.SERVICE_TYPE_EC2, timestamp, unit, value, virt_type)
            try:
                mysql_cursor.execute(s.ADD_EC2_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                # continue
                print("MYSQL Error: {}".format(err))
    return


def insert_elb_datapoints_to_db(load_balancer_name, datapoints, metric, mysql_cursor, region):
    unit, stat_type = c.ELB_METRIC_UNIT_DICTIONARY.get(metric)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = datapoint.get(stat_type)
        unit = datapoint.get('Unit')
        data = (c.ACCOUNT_NAME, load_balancer_name, metric, region,
                c.SERVICE_TYPE_ELB, timestamp, unit, value)
        try:
            mysql_cursor.execute(s.ADD_ELB_DATAPOINTS(), data)
        except mysql.connector.Error as err:
            print("MYSQL Error: {}".format(err))
    return


def insert_rds_datapoints_to_db(instance_name, instance_info, datapoints, metric, mysql_cursor, region):
    unit, stat_type = c.RDS_METRIC_UNIT_DICTIONARY.get(metric)
    multi_az = instance_info.get(c.COLUMN_NAME_RDS_MULTI_AZ)
    security_groups = instance_info.get(c.COLUMN_NAME_RDS_SECURITY_GRP)
    engine = instance_info.get(c.COLUMN_NAME_RDS_ENGINE)
    instance_class = instance_info.get(c.COLUMN_NAME_RDS_INSTANCE_CLASS)
    for datapoint in datapoints:
        timestamp = datapoint.get('Timestamp')
        value = datapoint.get(stat_type)
        unit = datapoint.get('Unit')
        for securityGroup in security_groups:
            security_group_string = str(securityGroup)
            data = (c.ACCOUNT_NAME, engine, instance_class, metric, multi_az,
                    instance_name, region, security_group_string, timestamp, unit, value)
            try:
                mysql_cursor.execute(s.ADD_RDS_DATAPOINTS(), data)
            except mysql.connector.Error as err:
                print("MYSQL Error: {}".format(err))
    return


def get_all_datapoints(metrics, service):
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
    filtered_list = filter(func.exists, datalist)
    return filtered_list


def insert_ec2_metrics_to_db(metrics, security_grp_dict, instance_info, mysql_conn, region):
    mysql_cursor = mysql_conn.cursor()
    func.create_ec2_table(mysql_cursor)
    data_list = get_all_datapoints(metrics, c.SERVICE_TYPE_EC2)
    for metric, datapoints in data_list:
        instance_id = metric.dimensions.get('InstanceId')[0].replace('-', '_')
        if instance_info.get(instance_id) is None:
            continue
        else:
            security_groups = security_grp_dict.get(instance_id)
            metric_string = str(metric)[7:]
            metric_tuple = (instance_id, metric_string) + instance_info.get(instance_id)
            insert_ec2_datapoints_to_db(datapoints, security_groups, metric_tuple, mysql_cursor, region)
    return


def insert_elb_metrics_to_db(metrics, mysql_conn, region):
    mysql_cursor = mysql_conn.cursor()
    func.create_elb_table(mysql_cursor)
    data_list = get_all_datapoints(metrics, c.SERVICE_TYPE_ELB)
    for metric, datapoints in data_list:
        load_balancer_name = metric.dimensions.get('LoadBalancerName')[0].replace('-', '_')
        insert_elb_datapoints_to_db(load_balancer_name, datapoints, str(metric)[7:], mysql_cursor, region)
    return


def insert_rds_metrics_to_db(metrics, instance_dict, mysql_conn, region):
    mysql_cursor = mysql_conn.cursor()
    func.create_rds_table(mysql_cursor)
    data_list = get_all_datapoints(metrics, c.SERVICE_TYPE_RDS)
    for metric, datapoints in data_list:
        instance_name = metric.dimensions.get('DBInstanceIdentifier')[0].replace('-', '_')
        if datapoints is None:
            continue
        else:
            instance_info = instance_dict.get(instance_name)
            insert_rds_datapoints_to_db(instance_name, instance_info, datapoints,
                                        str(metric)[7:], mysql_cursor, region)
    return


def build_ec2_security_grp_dictionary(ec2connection):
    security_groups = ec2connection.get_all_security_groups()
    security_grp_dict = {}
    for group in security_groups:
        instances = group.instances()
        if instances:
            for instance in instances:
                if instance.state == 'running':
                    instance_id = str(instance)[9:].replace('-', '_')
                    security_grp_list = security_grp_dict.get(instance_id)
                    if security_grp_list is None:
                        security_grp_list = [group]
                        security_grp_dict[instance_id] = security_grp_list
                    else:
                        security_grp_list.append(group)
                        security_grp_dict[instance_id] = security_grp_list
    return security_grp_dict


def extract_ec2_instances(connection):
    filter = {'instance-state-name': 'running'}
    reservation_list = connection.get_all_instances(filters=filter)
    instance_info = {}
    instance_list = []
    for reservation in reservation_list:
        instance = reservation.instances[0]
        instance_tuple = (instance.virtualization_type, instance.instance_type,
                          instance.key_name, instance.image_id.replace('-', '_'))
        instance_info[instance.id.replace('-', '_')] = instance_tuple
        instance_list.append(instance.id)
    return instance_list, instance_info


def extract_db_instances(rds_conn):
    instances = rds_conn.get_all_dbinstances()
    instance_dict = {}
    for instance in instances:
        instance_info = {c.COLUMN_NAME_RDS_MULTI_AZ: instance.multi_az,
                         c.COLUMN_NAME_RDS_SECURITY_GRP: instance.security_groups,
                         c.COLUMN_NAME_RDS_ENGINE: instance.engine,
                         c.COLUMN_NAME_RDS_INSTANCE_CLASS: instance.instance_class
                         }
        instance_dict[instance.id] = instance_info
    return instance_dict


def add_all_ec2_datapoints(ec2_conn, cw_conn, mysql_conn, region):
    security_grp_dictionary = build_ec2_security_grp_dictionary(ec2_conn)
    instance_list, instance_info = extract_ec2_instances(ec2_conn)
    metrics = get_all_metrics(cw_conn, c.SERVICE_TYPE_EC2)
    insert_ec2_metrics_to_db(metrics, security_grp_dictionary, instance_info, mysql_conn, region)
    return


def add_all_elb_datapoints(cw_conn, mysql_conn, region):
    metrics = get_all_metrics(cw_conn, c.SERVICE_TYPE_ELB)
    insert_elb_metrics_to_db(metrics, mysql_conn, region)
    return


def add_all_rds_datapoints(rds_conn, cw_conn, mysql_conn, region):
    metrics = get_all_metrics(cw_conn, c.SERVICE_TYPE_RDS)
    instance_dict = extract_db_instances(rds_conn)
    insert_rds_metrics_to_db(metrics, instance_dict, mysql_conn, region)
    return


def execute(region):
    start_time = time.time()
    region_string = region.replace("-", "_")
    ec2_conn, rds_conn, cw_conn, mysql_conn = initialise_connections(region)
    # ec2Start = time.time()
    add_all_ec2_datapoints(ec2_conn, cw_conn, mysql_conn, region_string)
    # ec2End = time.time()
    # print "Time taken to add EC2 Datapoints (" + c.ACCOUNT_NAME + ' ' + region + "): " + str(ec2End - ec2Start)
    add_all_elb_datapoints(cw_conn, mysql_conn, region_string)
    # elbEnd = time.time()
    # print "Time taken to add ELB Datapoints: (" + c.ACCOUNT_NAME + ' ' + region + "): " + str(elbEnd - ec2End)
    add_all_rds_datapoints(rds_conn, cw_conn, mysql_conn, region_string)
    # print "Time taken to add RDS Datapoints: (" + c.ACCOUNT_NAME + ' ' + region + "): " + str(time.time() - elbEnd)
    mysql_conn.commit()
    mysql_conn.close()
    print "Execution Time: (" + c.ACCOUNT_NAME + ' ' + region + "): " + str(time.time() - start_time)


if __name__ == "__main__":
    startTime = time.time()
    pool = MyPool(c.REGION_POOL_SIZE)
    pool.map(execute, c.REGION_LIST)
    print "Total Execution Time: (" + c.ACCOUNT_NAME + "): " + str(time.time() - startTime)
