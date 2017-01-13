echo "Generating Passwordless Connection for hadoop user (Multinode)"
args=$#
hadoop_user=$1
hadoop_pass=$2
root_pass=$3
i=4
shift 3
while [ $i -le $args ]
do
        sudo -u $hadoop_user -H sh -c "echo $1 >> /usr/local/hadoop/slaves_ip.dat"
	sudo -u $hadoop_user -H sh -c "sshpass -p "$hadoop_pass" ssh-copy-id -o StrictHostKeyChecking=no -i /home/"$hadoop_user"/.ssh/id_rsa.pub $1"
        shift
        i=`expr $i + 1`
done
i=1
echo "Backing up hosts file in /etc/"
cp /etc/hosts /etc/hosts.bak
rm /etc/hosts
echo "Backing up hostname file in /etc/"
cp /etc/hostname /etc/hostname.bak
rm /etc/hostname
mas_ip=`ifconfig | grep 'broadcast\|Bcast' | awk -F ' ' {'print $2'} | head -n 1 | sed -e 's/addr://g'`
echo $mas_ip    hadoop-master >> /etc/hosts
echo hadoop-master >> /etc/hostname
sudo service hostname restart
n=`grep -c '' /usr/local/hadoop/slaves_ip.dat`
n=`expr $n + 1`
l=1
for i in `cat /usr/local/hadoop/slaves_ip.dat`
do
	echo $i hadoop-slave-$l >> /etc/hosts
	l=`expr $l + 1`
done
for slave_ip in `cat /usr/local/hadoop/slaves_ip.dat`
do
	sshpass -p $root_pass ssh-copy-id -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa.pub root@$slave_ip
done
echo "Transferring the hostname file to respective slaves"
k=1
while [ $k -lt $n ]
do
	echo hadoop-slave-$k >> hostname_$k
	sshpass -p $root_pass scp -o StrictHostKeyChecking=no hostname_$k hadoop-slave-$k:/etc/hostname
	rm hostname_$k
	k=`expr $k + 1`
done
cd /etc/
echo "Transferring the hosts file to slave nodes"
j=1
while [ $j -lt $n ]
do
	sshpass -p $root_pass scp -o StrictHostKeyChecking=no -r hosts hadoop-slave-$j:/etc/
	j=`expr $j + 1`
done
echo "Updating the Hadoop Configuration files"
echo "Updating the core-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; perl -0pe 's/<property>\n<name>fs.default.name<\/name>\n<value>hdfs:\/\/localhost:9000<\/value>\n<\/property>/<property>\n<name>fs.default.name<\/name>\n<value>hdfs:\/\/hadoop-master:54310\/<\/value>\n<\/property> /' core-site.xml > tmp_core-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; mv tmp_core-site.xml core-site.xml"
echo "Updating the hdfs-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; perl -0pe 's/<property>\n<name>dfs.replication<\/name>\n<value>1<\/value>\n<\/property>\n\n<property>\n<name>dfs.name.dir<\/name>\n<value> file:\/\/\/home\/'"$hadoop_user"'\/hadoopinfra\/hdfs\/namenode <\/value>\n<\/property>\n\n<property>\n<name>dfs.data.dir<\/name>\n<value> file:\/\/\/home\/'"$hadoop_user"'\/hadoopinfra\/hdfs\/datanode <\/value>\n<\/property>/<property>\n<name>dfs.data.dir<\/name>\n<value>\/usr\/local\/hadoop\/dfs\/name\/data<\/value>\n<final>true<\/final>\n<\/property>\n\n<property>\n<name>dfs.name.dir<\/name>\n<value>\/usr\/local\/hadoop\/dfs\/name<\/value>\n<final>true<\/final>\n<\/property>\n\n<property>\n<name>dfs.replication<\/name>\n<value>3<\/value>\n<\/property>/' hdfs-site.xml > tmp_hdfs-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; mv tmp_hdfs-site.xml hdfs-site.xml"
echo "Updating the mapred-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; perl -0pe 's/<property>\n<name>mapreduce.framework.name<\/name>\n<value>yarn<\/value>\n<\/property>/<property>\n<name>mapred.job.tracker<\/name>\n<value>hadoop-master:54311<\/value>\n<\/property>\n<property>\n<name>mapreduce.framework.name<\/name>\n<value>yarn<\/value>\n<\/property>\n<property>\n<name>mapreduce.jobhistory.address<\/name>\n<value>hadoop-master:10020<\/value>\n<\/property>\n<property>\n<name>mapreduce.jobhistory.webapp.address<\/name>\n<value>hadoop-master:19888<\/value>\n<\/property>/' mapred-site.xml > tmp_mapred-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; mv tmp_mapred-site.xml mapred-site.xml"
echo "Updating the yarn-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; sed '/^<configuration>$/a \n<property>\n<name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>\n<value>org.apache.hadoop.mapred.ShuffleHandler</value>\n</property>\n<property>\n<name>yarn.resourcemanager.resource-tracker.address</name>\n<value>hadoop-master:8025</value>\n</property>\n<property>\n<name>yarn.resourcemanager.scheduler.address</name>\n<value>hadoop-master:8030</value>\n</property>\n<property>\n<name>yarn.resourcemanager.address</name>\n<value>hadoop-master:8050</value>\n</property>' yarn-site.xml > tmp_yarn-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; mv tmp_yarn-site.xml yarn-site.xml"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; echo export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop >> hadoop-env.sh"
sudo -u $hadoop_user -H sh -c "cd /usr/local/"
echo "Transferring the hadoop settings to slave nodes"
n=`grep -c '' /usr/local/hadoop/slaves_ip.dat`
i=1
n=`expr $n + 1`
while [ $i -lt $n ]
do
        sudo -u $hadoop_user -H sh -c "scp -r /usr/local/hadoop hadoop-slave-"$i":/usr/local"
        i=`expr $i + 1`
done
echo "Updating the master settings and slave settings in master & slave file respectively in master node only"
sudo -u $hadoop_user -H sh -c "cd /usr/local/hadoop/etc/hadoop; echo hadoop-master >> masters; rm slaves; echo hadoop-master >> slaves"
j=1
while [ $j -lt $n ]
do
	sudo -u $hadoop_user -H sh -c "echo hadoop-slave-"$j" >> /usr/local/hadoop/etc/hadoop/slaves"
	j=`expr $j + 1`
done
echo "Starting the HDFS"
sudo -u $hadoop_user -H sh -c "/usr/local/hadoop/bin/hdfs namenode -format -force"
sudo -u $hadoop_user -H sh -c "/usr/local/hadoop/sbin/start-dfs.sh"
sudo -u $hadoop_user -H sh -c "/usr/local/hadoop/sbin/start-yarn.sh"
sudo -u $hadoop_user -H sh -c "/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver"
exit
