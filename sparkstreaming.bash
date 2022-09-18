wget http://media.sundog-soft.com/hadoop/sparkstreamingflume.conf
wget http://media.sundog-soft.com/hadoop/SparkFlume.py

mkdir checkpoint
spark-submit --packages org.apache.spark:spark-streaming-flume_2.11:2.3.0 SparkFlume.py

cd /usr/hdp/current/flume-server/
bin/flume-ng agent --conf conf --conf-file ~/sparkstreamingflume.conf --name a1

wget http://media.sundog-soft.com/hadoop/access_log.txt
cp access_log.txt spool/log22.txt

