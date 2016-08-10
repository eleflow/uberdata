#!/usr/bin/env bash

sudo bash spark-jars.sh
sudo chmod -R 777 /opt/spark

zeppelinVersion="0.6.0"
sparkVersion="1.6.2"
mySqlConnectorVersion="5.1.34"
zeppelinInterpreterUberdataDir="/usr/share/zeppelin/interpreter/uberdata/"

sudo cp iuberdata /etc/init.d/iuberdata

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

cd $SCRIPTPATH
cd ../..

echo "clonning and building spark-timeseries"
git clone https://github.com/sryza/spark-timeseries.git
sudo chmod 777 +x spark-timeseries
cd spark-timeseries
mvn package install -DskipTests -Dgpg.skip

cd $SCRIPTPATH
cd ../

echo "assembly iuberdata_zeppelin"
sbt "project iuberdata_zeppelin" assembly
echo "assembly iuberdata_addon_zeppelin"
sbt "project iuberdata_addon_zeppelin" assembly

# install Zeppelin
echo "downloading zeppelin"
sudo cp -rf /usr/share/zeppelin/notebook /tmp/
sudo cp -f /usr/share/zeppelin/conf/interpreter.json /tmp/notebook

sudo rm -rf /usr/share/zeppelin*

wget http://www-us.apache.org/dist/zeppelin/zeppelin-$zeppelinVersion/zeppelin-$zeppelinVersion-bin-all.tgz -O /tmp/zeppelin-$zeppelinVersion.tgz

sudo tar -xzvf /tmp/zeppelin-$zeppelinVersion.tgz -C /usr/share
sudo rm -f /tmp/zeppelin-$zeppelinVersion.tgz
sudo find /usr/share -name zeppelin-* -type d -exec ln -s {} /usr/share/zeppelin \;
sudo rm -rf /usr/share/zeppelin/notebook/*

sudo mkdir -p $zeppelinInterpreterUberdataDir

cd $zeppelinInterpreterUberdataDir

sudo cp ../spark/zeppelin-spark*jar ./
sudo rm -rf ../spark
sudo rm -rf ../hive
sudo rm -rf ../tajo

sudo ln -s /opt/spark/lib/spark-assembly* ./spark-assembly.jar
sudo ln -s /opt/spark/lib/datanucleus-core*.jar ./datanucleus-core.jar
sudo ln -s /opt/spark/lib/datanucleus-api*.jar ./datanucleus-api.jar
sudo ln -s /opt/spark/lib/datanucleus-rdbms*.jar ./datanucleus-rdbms.jar

cd $SCRIPTPATH

echo $SCRIPTPATH

sudo cp $SCRIPTPATH/zeppelin-site.xml /usr/share/zeppelin/conf/

sudo cp $SCRIPTPATH/interpreter.sh /usr/share/zeppelin/bin/
sudo chmod 777 /usr/share/zeppelin/bin/interpreter.sh

# install IUberdata
echo "preparing uberdata"
sudo cp -f ../iuberdata_zeppelin/target/scala-2.10/eleflow.uberdata.IUberdata-Zeppelin-0.1.0.jar $zeppelinInterpreterUberdataDir
sudo cp -f ../iuberdata_addon_zeppelin/target/scala-2.10/iuberdata_addon_zeppelin-assembly-0.1.0.jar /usr/share/zeppelin/lib/

sudo unzip shell.zip -d /usr/share/zeppelin/notebook

sudo cp -f /tmp/notebook/interpreter.json /usr/share/zeppelin/conf/
sudo rm -f /tmp/notebook/interpreter.json
sudo cp -rf /tmp/notebook/* /usr/share/zeppelin/notebook
sudo rm -rf /tmp/notebook*

cd ../..

#download and deploy mysql-connector-java
echo "download and deploy mysql connector"
sudo wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-$mySqlConnectorVersion.tar.gz -O /tmp/mysql-connector-java-$mySqlConnectorVersion.tar.gz
sudo tar -xzvf /tmp/mysql-connector-java-$mySqlConnectorVersion.tar.gz -C /tmp
sudo mv /tmp/mysql-connector-java-$mySqlConnectorVersion/mysql-connector-java-$mySqlConnectorVersion-bin.jar $zeppelinInterpreterUberdataDir
sudo rm -rf /tmp/mysql-connector-java-$mySqlConnectorVersion

sudo chmod 777 -R /opt/spark
sudo chmod 777 -R /usr/share/zeppelin-*