#!/bin/sh

sudo apt-get -y install python-pip
sudo pip install -U boto
sudo apt-get -y install python-mysql.connector
sudo apt-get build-dep python-mysqldb
sudo pip install MySQL-python
sudo pip install elasticsearch
sudo apt-get install mysql-server