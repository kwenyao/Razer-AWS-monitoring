sudo vim /etc/elasticsearch/elasticsearch.yml 
- make sure the cluster names for all nodes are the same.
- uncomment the line 'node.name' and give each node a unique name (e.g. node1)
- uncomment 'discovery.zen.ping.multicast.enabled: false'
- uncomment 'discovery.zen.ping.unicast.hosts: ["node2","node3"]' <<< in the list add the node names of all the other nodes


sudo vim /etc/hosts
- add in the ip addresses and node names of all the nodes.
- an example of the file (/etc/hosts):

	127.0.0.1 localhost
	10.181.133.225 node1 <<< THIS LINE WAS ADDED
	10.168.13.137 node2  <<< THIS LINE WAS ADDED
	10.158.53.188 node3  <<< THIS LINE WAS ADDED

	# The following lines are desirable for IPv6 capable hosts
	::1 ip6-localhost ip6-loopback
	fe00::0 ip6-localnet
	ff00::0 ip6-mcastprefix
	ff02::1 ip6-allnodes
	ff02::2 ip6-allrouters
	ff02::3 ip6-allhosts
