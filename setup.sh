#!/bin/sh

IP_ADDR='0.0.0.0'
CLUSTER_NAME='elasticsearch'
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get -y install oracle-java8-installer
wget -O - http://packages.elasticsearch.org/GPG-KEY-elasticsearch | sudo apt-key add -
echo 'deb http://packages.elasticsearch.org/elasticsearch/1.5/debian stable main' | sudo tee /etc/apt/sources.list.d/elasticsearch.list
sudo apt-get update
sudo apt-get -y install elasticsearch=1.5.2
sudo sed -i "s/#network.host: 192.168.0.1/network.host: $IP_ADDR/g" /etc/elasticsearch/elasticsearch.yml
sudo sed -i "s/#cluster.name: elasticsearch/cluster.name: $CLUSTER_NAME/g" /etc/elasticsearch/elasticsearch.yml
sudo service elasticsearch restart
sudo update-rc.d elasticsearch defaults 95 10
cd ~; wget https://download.elasticsearch.org/kibana/kibana/kibana-4.0.2-linux-x64.tar.gz
tar xvf kibana-*.tar.gz
#sed -i 's/host: "0.0.0.0"/host: "localhost"/g' ~/kibana-4*/config/kibana.yml
sudo mkdir -p /opt/kibana
sudo cp -R ~/kibana-4*/* /opt/kibana/
cd /etc/init.d && sudo wget https://gist.githubusercontent.com/thisismitch/8b15ac909aed214ad04a/raw/bce61d85643c2dcdfbc2728c55a41dab444dca20/kibana4
sudo chmod +x /etc/init.d/kibana4
sudo update-rc.d kibana4 defaults 96 9
sudo service kibana4 start
