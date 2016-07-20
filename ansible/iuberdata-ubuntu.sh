#!/bin/bash


version="0.1.0"

# java-devel
 sudo add-apt-repository -y ppa:webupd8team/java
 sudo apt-get -y update
 sudo apt-get -y install oracle-java8-installer
#alternatives --set java  /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
sudo -E bash -c 'echo $JAVA_HOME'
export SPARK_HOME=/opt/spark
sudo -E bash -c 'echo $SPARK_HOME'
# Adding system user/group : iuberdata and iuberdata
if ! getent group | grep -q "^iuberdata:" ;
then
    echo "Creating system group: iuberdata"
    groupadd iuberdata
fi
if ! getent passwd | grep -q "^iuberdata:";
then
    echo "Creating system user: iuberdata"
    useradd --gid iuberdata --create-home --comment "Uberdata Interactive User" iuberdata
fi

# install Zeppelin
aws s3 cp s3://uberdata-public/zeppelin/ /tmp/ --recursive --include "zeppelin-*.tar.gz"

tar xzvf /tmp/zeppelin-*.tar.gz -C /usr/share

rm -f /tmp/zeppelin-*.tar.gz
rm -rf /user/share/zeppelin
find /usr/share -name 'zeppelin-*' -type d -exec ln -s {} /usr/share/zeppelin \;

mkdir -p /usr/share/zeppelin/interpreter/uberdata
cp /usr/share/zeppelin/interpreter/spark/zeppelin-spark* /usr/share/zeppelin/interpreter/uberdata/

rm -rf /usr/share/zeppelin/interpreter/spark
rm -rf /usr/share/zeppelin/interpreter/hive
rm -rf /usr/share/zeppelin/interpreter/tajo

mv interpreter.sh /usr/share/zeppelin/bin/
chmod +x /usr/share/zeppelin/bin/interpreter.sh
#chown iuberdata:iuberdata /usr/share/zeppelin/conf/zeppelin-site.xml

# install IUberdata

file="/tmp/eleflow.uberdata.IUberdata-Zeppelin-$version.jar"
if ! test -f "$file"
then
    aws s3 cp s3://uberdata-artifactory/eleflow.uberdata.IUberdata-Zeppelin-$version.jar /tmp
fi
rm -rf /usr/share/zeppelin/interpreter/uberdata/*
cp /tmp/eleflow.uberdata.IUberdata-Zeppelin-0.1.0.jar /usr/share/zeppelin/interpreter/uberdata/

ln -s /opt/spark/lib/spark-assembly* /usr/share/zeppelin/interpreter/uberdata/spark-assembly.jar
ln -s /opt/spark/lib/datanucleus-core*.jar /usr/share/zeppelin/interpreter/uberdata/datanucleus-core.jar
ln -s /opt/spark/lib/datanucleus-api*.jar /usr/share/zeppelin/interpreter/uberdata/datanucleus-api.jar
ln -s /opt/spark/lib/datanucleus-rdbms*.jar /usr/share/zeppelin/interpreter/uberdata/datanucleus-rdbms.jar


chown -R iuberdata:iuberdata  /usr/share/zeppelin-*
chown  iuberdata:iuberdata  /usr/share/zeppelin

#install init.d scripts
mkdir -p /var/log/iuberdata
chown iuberdata:iuberdata /var/log/iuberdata
mkdir -p /etc/default/iuberdata
chown iuberdata:iuberdata /etc/default/iuberdata
mkdir -p /var/run/iuberdata
chown iuberdata:iuberdata /var/run/iuberdata



chmod +x /etc/init.d/iuberdata





