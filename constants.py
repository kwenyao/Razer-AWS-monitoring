__author__ = 'Koh Wen Yao'

from collections import OrderedDict

########################################################################
# Duration/Interval Constants
########################################################################

SCRIPT_RUN_INTERVAL_MINUTES = 3
DATA_RETRIEVAL_BUFFER_MINUTES = 5
MONITORING_TIME_MINUTES = SCRIPT_RUN_INTERVAL_MINUTES
DATA_RETRIEVAL_TIME_DELTA_MINUTES = SCRIPT_RUN_INTERVAL_MINUTES + DATA_RETRIEVAL_BUFFER_MINUTES


########################################################################
# Authentication Constants
########################################################################

# ACCESS_KEY_ID = 'AKIAIK7ZZMC6W7GM4SRA' # Test server
# SECRET_ACCESS_KEY = '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' # Test Server
ACCESS_KEY_ID = 'AKIAIVATWMBLGQRVYWRA'
SECRET_ACCESS_KEY = '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'
ACCOUNT_NAME = 'emily'
EC2_REGION = 'us-east-1'
ELB_REGION = EC2_REGION
RDS_REGION = EC2_REGION
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_HOST = 'localhost'


########################################################################
# Database Constants
########################################################################

DATABASE_NAME = 'monitoring'
TABLE_NAME_EC2 = 'ec2datapoints'
TABLE_NAME_ELB = 'elbdatapoints'
COLUMN_NAME_EC2_ACCOUNT_NAME = 'account_name'
COLUMN_NAME_EC2_AMI_ID = 'ami_id'
COLUMN_NAME_EC2_INSTANCE_ID = 'instance_id'
COLUMN_NAME_EC2_INSTANCE_TYPE = 'instance_type'
COLUMN_NAME_EC2_KEY_NAME = 'key_name'
COLUMN_NAME_EC2_METRIC = 'metric'
COLUMN_NAME_EC2_REGION = 'region'
COLUMN_NAME_EC2_SECURITY_GRP = 'security_group'
COLUMN_NAME_EC2_SERVICE_TYPE = 'service_type'
COLUMN_NAME_EC2_TIMESTAMP = 'timestamp'
COLUMN_NAME_EC2_UNIT = 'unit'
COLUMN_NAME_EC2_VALUE = 'value'
COLUMN_NAME_EC2_VIRT_TYPE = 'virtualization_type'

COLUMN_NAME_ELB_ACCOUNT_NAME = 'account_name'
COLUMN_NAME_ELB_LOAD_BALANCER_NAME = 'load_balancer_name'
COLUMN_NAME_ELB_METRIC = 'metric'
COLUMN_NAME_ELB_REGION = 'region'
COLUMN_NAME_ELB_TIMESTAMP = 'timestamp'
COLUMN_NAME_ELB_SERVICE_TYPE = 'service_type'
COLUMN_NAME_ELB_UNIT = 'unit'
COLUMN_NAME_ELB_VALUE = 'value'

COLUMN_NAME_RDS_ACCOUNT_NAME = 'account_name'
COLUMN_NAME_RDS_ENGINE = 'engine'
COLUMN_NAME_RDS_INSTANCE_CLASS = 'instance_class'
COLUMN_NAME_RDS_METRIC = 'metric'
COLUMN_NAME_RDS_MULTI_AZ = 'multi_az'
COLUMN_NAME_RDS_NAME = 'name'
COLUMN_NAME_RDS_REGION = 'region'
COLUMN_NAME_RDS_SECURITY_GRP = 'security_group'
COLUMN_NAME_RDS_TIMESTAMP = 'timestamp'
COLUMN_NAME_RDS_VALUE = 'value'

PRIMARY_KEYS_EC2 = [COLUMN_NAME_EC2_ACCOUNT_NAME,
                    COLUMN_NAME_EC2_REGION,
                    COLUMN_NAME_EC2_INSTANCE_ID,
                    COLUMN_NAME_EC2_METRIC,
                    COLUMN_NAME_EC2_SECURITY_GRP,
                    COLUMN_NAME_EC2_TIMESTAMP]

PRIMARY_KEYS_ELB = [COLUMN_NAME_ELB_ACCOUNT_NAME,
                    COLUMN_NAME_ELB_REGION,
                    COLUMN_NAME_ELB_LOAD_BALANCER_NAME,
                    COLUMN_NAME_ELB_METRIC,
                    COLUMN_NAME_ELB_TIMESTAMP]

PRIMARY_KEYS_RDS = [COLUMN_NAME_ELB_ACCOUNT_NAME,
                    COLUMN_NAME_RDS_REGION,
                    COLUMN_NAME_RDS_NAME,
                    COLUMN_NAME_RDS_METRIC,
                    COLUMN_NAME_RDS_SECURITY_GRP,
                    COLUMN_NAME_RDS_TIMESTAMP]


########################################################################
# Addresses/Ports Constants
########################################################################

ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = '9200'
ELASTICSEARCH_URL = 'http://' + ELASTICSEARCH_HOST + ':' + ELASTICSEARCH_PORT
KIBANA_PORT = "5601"


########################################################################
# Service Constants
########################################################################

EC2_METRIC_STAT_TYPE = 'Average'  # 'Minimum', 'Maximum', 'Sum', 'Average', 'SampleCount'
SERVICE_TYPE_EC2 = 'EC2'
SERVICE_TYPE_ELB = 'ELB'
SERVICE_TYPE_RDS = 'RDS'


########################################################################
# Elasticsearch Constants
########################################################################

ELASTICSEARCH_INDEX_NAME = 'monitoring'


########################################################################
# Dictionaries
########################################################################

NAMESPACE_DICTIONARY = {SERVICE_TYPE_EC2: "AWS/EC2",
                        SERVICE_TYPE_ELB: "AWS/ELB",
                        SERVICE_TYPE_RDS: "AWS/RDS"
                        }

PRIMARY_KEY_DICTIONARY = {SERVICE_TYPE_EC2: PRIMARY_KEYS_EC2,
                          SERVICE_TYPE_ELB: PRIMARY_KEYS_ELB,
                          }

EC2_METRIC_UNIT_DICTIONARY = {"CPUCreditBalance": (None, 'Average'),
                              "CPUCreditUsage": (None, 'Average'),
                              "CPUUtilization": ('Percent', 'Average'),
                              "DiskReadBytes": ('Bytes', 'Average'),
                              "DiskReadOps": (None, 'Average'),
                              "DiskWriteBytes": ('Bytes', 'Average'),
                              "DiskWriteOps": (None, 'Average'),
                              "NetworkIn": ('Bytes', 'Average'),
                              "NetworkOut": ('Bytes', 'Average'),
                              "StatusCheckFailed_Instance": (None, 'Average'),
                              "StatusCheckFailed_System": (None, 'Average'),
                              "StatusCheckFailed": (None, 'Average'),
                              "VolumeIdleTime": (None, 'Average'),
                              "VolumeQueueLength": (None, 'Average'),
                              "VolumeReadBytes": (None, 'Average'),
                              "VolumeReadOps": (None, 'Average'),
                              "VolumeTotalReadTime": (None, 'Average'),
                              "VolumeTotalWriteTime": (None, 'Average'),
                              "VolumeWriteBytes": (None, 'Average'),
                              "VolumeWriteOps": (None, 'Average')
                              }

ELB_METRIC_UNIT_DICTIONARY = {"BackendConnectionErrors": (None, 'Sum'),
                              "HealthyHostCount": ('Count', 'Average'),
                              "HTTPCode_Backend_2XX": ('Count', 'Sum'),
                              "HTTPCode_Backend_3XX": (None, 'Sum'),
                              "HTTPCode_Backend_4XX": ('Count', 'Sum'),
                              "HTTPCode_Backend_5XX": ('Count', 'Sum'),
                              "HTTPCode_ELB_4XX": (None, 'Sum'),
                              "HTTPCode_ELB_5XX": ('Count', 'Sum'),
                              "Latency": ('Seconds', 'Average'),
                              "RequestCount": ('Count', 'Sum'),
                              "SpilloverCount": (None, 'Sum'),
                              "SurgeQueueLength": (None, 'Sum'),
                              "UnHealthyHostCount": ('Count', 'Average')
                              }

RDS_METRIC_UNIT_DICTIONARY = {"BinLogDiskUsage": ('Bytes', 'Average'),
                              "CPUUtilization": ('Percent', 'Average'),
                              "CPUCreditUsage":  (None, 'Average'),
                              "CPUCreditBalance": (None, 'Average'),
                              "DatabaseConnections": ('Count', 'Average'),
                              "DiskQueueDepth": ('Count', 'Average'),
                              "FreeableMemory": ('Bytes', 'Average'),
                              "FreeStorageSpace": ('Bytes', 'Average'),
                              "ReplicaLag": ('Seconds', 'Average'),
                              "SwapUsage": ('Bytes', 'Average'),
                              "ReadIOPS": ('Count/Second', 'Average'),
                              "WriteIOPS": ('Count/Second', 'Average'),
                              "ReadLatency": ('Seconds', 'Average'),
                              "WriteLatency": ('Seconds', 'Average'),
                              "ReadThroughput": ('Bytes/Second', 'Average'),
                              "WriteThroughput": ('Bytes/Second', 'Average'),
                              "NetworkReceiveThroughput": ('Bytes/Second', 'Average'),
                              "NetworkTransmitThroughput": ('Bytes/Second', 'Average')}

EC2_DATAPOINT_ATTR_TYPE_DICTIONARY = OrderedDict([(COLUMN_NAME_EC2_ACCOUNT_NAME, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_EC2_AMI_ID, 'VARCHAR(16)',),
                                                  (COLUMN_NAME_EC2_INSTANCE_ID, 'VARCHAR(16)',),
                                                  (COLUMN_NAME_EC2_INSTANCE_TYPE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_EC2_KEY_NAME, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_EC2_METRIC, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_EC2_REGION, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_EC2_SECURITY_GRP, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_EC2_SERVICE_TYPE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_EC2_TIMESTAMP, 'DATETIME'),
                                                  (COLUMN_NAME_EC2_UNIT, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_EC2_VALUE, 'FLOAT'),
                                                  (COLUMN_NAME_EC2_VIRT_TYPE, 'VARCHAR(16)')
                                                  ])

ELB_DATAPOINT_ATTR_TYPE_DICTIONARY = OrderedDict([(COLUMN_NAME_ELB_ACCOUNT_NAME, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_ELB_LOAD_BALANCER_NAME, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_ELB_METRIC, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_ELB_REGION, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_ELB_SERVICE_TYPE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_ELB_TIMESTAMP, 'DATETIME'),
                                                  (COLUMN_NAME_ELB_UNIT, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_ELB_VALUE, 'FLOAT'),
                                                  ])

RDS_DATAPOINT_ATTR_TYPE_DICTIONARY = OrderedDict([(COLUMN_NAME_RDS_ACCOUNT_NAME, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_RDS_ENGINE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_RDS_INSTANCE_CLASS, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_RDS_METRIC, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_RDS_MULTI_AZ, 'VARCHAR(5)'),
                                                  (COLUMN_NAME_RDS_NAME, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_RDS_REGION, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_RDS_SECURITY_GRP, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_RDS_TIMESTAMP, 'DATETIME'),
                                                  (COLUMN_NAME_RDS_VALUE, 'FLOAT'),
                                                  ])
