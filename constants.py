__author__ = 'Koh Wen Yao'

from collections import OrderedDict

########################################################################
# Duration/Interval Constants
########################################################################

SCRIPT_RUN_INTERVAL_MINUTES = 1
DATA_RETRIEVAL_BUFFER_MINUTES = 2
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
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_HOST = 'localhost'


########################################################################
# Database Constants
########################################################################

DATABASE_NAME = 'monitoring'
EC2_METRICS_TABLE_NAME = 'ec2datapoints'
COLUMN_NAME_ACCOUNT_NAME = 'account_name'
COLUMN_NAME_AMI_ID = 'ami_id'
COLUMN_NAME_INSTANCE_ID = 'instance_id'
COLUMN_NAME_INSTANCE_TYPE = 'instance_type'
COLUMN_NAME_KEY_NAME = 'key_name'
COLUMN_NAME_METRIC = 'metric'
COLUMN_NAME_REGION = 'region'
COLUMN_NAME_SECURITY_GRP = 'security_group'
COLUMN_NAME_SERVICE_TYPE = 'service_type'
COLUMN_NAME_TIMESTAMP = 'timestamp'
COLUMN_NAME_UNIT = 'unit'
COLUMN_NAME_VALUE = 'value'
COLUMN_NAME_VIRT_TYPE = 'virtualization_type'
PRIMARY_KEYS = COLUMN_NAME_INSTANCE_ID + ', ' + \
               COLUMN_NAME_METRIC + ', ' + \
               COLUMN_NAME_TIMESTAMP + ', ' + \
               COLUMN_NAME_SECURITY_GRP


########################################################################
# Addresses/Ports Constants
########################################################################

ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = '9200'
ELASTICSEARCH_URL = 'http://' + ELASTICSEARCH_HOST + ':' + ELASTICSEARCH_PORT
KIBANA_PORT = "5601"


########################################################################
# EC2 Constants
########################################################################

EC2_METRIC_STAT_TYPE = 'Average'  # 'Minimum', 'Maximum', 'Sum', 'Average', 'SampleCount'
EC2_SERVICE_TYPE = 'EC2'


########################################################################
# Elasticsearch Constants
########################################################################

ELASTICSEARCH_INDEX_NAME = 'monitoring'


########################################################################
# Dictionaries
########################################################################

EC2_METRIC_UNIT_DICTIONARY = {"CPUCreditBalance": None,
                              "CPUCreditUsage": None,
                              "CPUUtilization": 'Percent',
                              "DiskReadBytes": 'Bytes',
                              "DiskReadOps": None,
                              "DiskWriteBytes": 'Bytes',
                              "DiskWriteOps": None,
                              "NetworkIn": 'Bytes',
                              "NetworkOut": 'Bytes',
                              "StatusCheckFailed_Instance": None,
                              "StatusCheckFailed_System": None,
                              "StatusCheckFailed": None,
                              "VolumeIdleTime": None,
                              "VolumeQueueLength": None,
                              "VolumeReadBytes": None,
                              "VolumeReadOps": None,
                              "VolumeTotalReadTime": None,
                              "VolumeTotalWriteTime": None,
                              "VolumeWriteBytes": None,
                              "VolumeWriteOps": None
                              }

EC2_DATAPOINT_ATTR_TYPE_DICTIONARY = OrderedDict([(COLUMN_NAME_ACCOUNT_NAME, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_AMI_ID, 'VARCHAR(16)',),
                                                  (COLUMN_NAME_INSTANCE_ID, 'VARCHAR(16)',),
                                                  (COLUMN_NAME_INSTANCE_TYPE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_KEY_NAME, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_METRIC, 'VARCHAR(32)'),
                                                  (COLUMN_NAME_REGION, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_SECURITY_GRP, 'VARCHAR(64)'),
                                                  (COLUMN_NAME_SERVICE_TYPE, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_TIMESTAMP, 'DATETIME'),
                                                  (COLUMN_NAME_UNIT, 'VARCHAR(16)'),
                                                  (COLUMN_NAME_VALUE, 'FLOAT'),
                                                  (COLUMN_NAME_VIRT_TYPE, 'VARCHAR(16)')
                                                  ])
