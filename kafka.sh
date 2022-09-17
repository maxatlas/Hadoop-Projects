cd /usr/hdp/current/kafka-broker/bin
# create topic
./kafka-topics.sh --create --zookeeper sandbox-hdp.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic test 
./kafka-topics.sh --list --zookeeper sandbox.hortonworks.com:2181
# listen to standard for keyboard input on topic test, 
./kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic test # 
cd ..
cd conf
cp connect-standalone.properties ~/
cp connect-file-source ~/
cp connect-file-sink ~/
cd
# bootstrap.server=sandbox-hdp.hortonworks.com:6667
vi connect-standalone.properties
# file=/home/maria_dev/access_log_small.txt
# topics=log-test
vi connect-file-source
# file=/home/maria_dev/logout.txt
# topics=log-test
vi connect-file-sink

wget http://media.sundog-soft.com/hadoop/access_log_small.txt
cd /usr/hdp/current/kafka-broker/bin/
./kafka-topics.sh --create --zookeeper sandbox-hdp.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic log-test
./kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic log-test
