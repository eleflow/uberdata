#!/bin/bash
yum -y update

yum -y groupinstall "Development Tools"

scalaVersion="2.10"
uberdataVersion="0.1.0"
mySqlConnectorVersion="5.1.34"
zeppelinVersion="0.6.0"
zeppelinInterpreterUberdataDir="/usr/share/zeppelin/interpreter/uberdata/"

# java-devel
yum -y install java-1.8.0-openjdk-devel
alternatives --set java  /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java

# install R
su -c 'rpm -Uvh http://download.fedoraproject.org/pub/epel/5/i386/epel-release-5-4.noarch.rpm'
yum -y update
yum -y install R

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

chown -R iuberdata:iuberdata /opt/spark

# install Zeppelin
cp -rf /usr/share/zeppelin/notebook /tmp/
cp -f /usr/share/zeppelin/conf/interpreter.json /tmp/notebook

rm -rf /usr/share/zeppelin*

#aws s3 cp s3://uberdata-public/zeppelin/ /tmp/ --recursive --include "zeppelin-*.tar.gz"

wget "http://www-us.apache.org/dist/zeppelin/zeppelin-$zeppelinVersion/zeppelin-$zeppelinVersion-bin-all.tgz" -O "/tmp/zeppelin-$zeppelinVersion.tgz"

tar -xzvf /tmp/zeppelin-*.tgz -C /usr/share
rm -f /tmp/zeppelin-*.tgz
rm -f /user/share/zeppelin
find /usr/share -name zeppelin-* -type d -exec ln -s {} /usr/share/zeppelin \;
rm -rf /usr/share/zeppelin/notebook/*

mkdir -p $zeppelinInterpreterUberdataDir

cd $zeppelinInterpreterUberdataDir

cp ../spark/zeppelin-spark*jar ./
rm -rf ../spark
rm -rf ../hive
rm -rf ../tajo
mv /tmp/zeppelin-site.xml /usr/share/zeppelin/conf/

mv /tmp/interpreter.sh /usr/share/zeppelin/bin/
chmod +x /usr/share/zeppelin/bin/interpreter.sh
#chown iuberdata:iuberdata /usr/share/zeppelin/conf/zeppelin-site.xml

aws s3 cp s3://uberdata-artifactory/iuberdata_addon_zeppelin-${uberdataVersion}.zip /tmp
unzip -o /tmp/iuberdata_addon_zeppelin*zip
cp -f /tmp/eleflow.iuberdata_addon_zeppelin-${uberdataVersion}/lib/eleflow.iuberdata_addon_zeppelin*.jar /usr/share/zeppelin/lib/

# install IUberdata
file="/tmp/eleflow.uberdata.IUberdata-Zeppelin-$uberdataVersion.jar"
if ! test -f "$file"
then
    aws s3 cp s3://uberdata-artifactory/eleflow.uberdata.IUberdata-Zeppelin-$uberdataVersion.jar /tmp
fi
cp /tmp/eleflow.uberdata.IUberdata-Zeppelin-$uberdataVersion.jar ./

ln -s /opt/spark/lib/spark-assembly* ./spark-assembly.jar
ln -s /opt/spark/lib/datanucleus-core*.jar ./datanucleus-core.jar
ln -s /opt/spark/lib/datanucleus-api*.jar ./datanucleus-api.jar
ln -s /opt/spark/lib/datanucleus-rdbms*.jar ./datanucleus-rdbms.jar

#install init.d scripts
mkdir -p /var/log/iuberdata
chown iuberdata:iuberdata /var/log/iuberdata
mkdir -p /etc/default/iuberdata
chown iuberdata:iuberdata /etc/default/iuberdata
mkdir -p /var/run/iuberdata
chown iuberdata:iuberdata /var/run/iuberdata

unzip /tmp/shell.zip -d /usr/share/zeppelin/notebook

cp -f /tmp/notebook/interpreter.json /usr/share/zeppelin/conf/
rm -f /tmp/notebook/interpreter.json
cp -rf /tmp/notebook/* /usr/share/zeppelin/notebook
rm -rf /tmp/notebook*

chmod +x /etc/init.d/iuberdata
chown -R iuberdata:iuberdata  /usr/share/zeppelin-*
chown  iuberdata:iuberdata  /usr/share/zeppelin

file="~/.ssh/id_rsa"
if ! sudo su - iuberdata -c "test $file"
 then
    sudo su - iuberdata -c "ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''"
    sudo su - iuberdata -c "ssh-agent ssh-add ~/.ssh/id_rsa"
 fi

#download and deploy mysql-connector-java
wget "http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-$mySqlConnectorVersion.tar.gz"
tar -xzvf "mysql-connector-java-$mySqlConnectorVersion.tar.gz" "mysql-connector-java-$mySqlConnectorVersion/mysql-connector-java-$mySqlConnectorVersion-bin.jar"
mv "mysql-connector-java-$mySqlConnectorVersion/mysql-connector-java-$mySqlConnectorVersion-bin.jar" ./
rm -rf "mysql-connector-java-$mySqlConnectorVersion"
rm -f "mysql-connector-java-$mySqlConnectorVersion.tar.gz"