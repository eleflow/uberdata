#!/usr/bin/env bash

sudo bash spark-jars.sh
sudo chmod -R 777 /opt/spark

# WARNING: changing the zeppelin version requires changing the dependency version in build.sbt and iuberdata.sh
zeppelinVersion="0.7.1"
sparkVersion="2.1.0"
mySqlConnectorVersion="5.1.34"
zeppelinInterpreterUberdataDir="/usr/share/zeppelin/interpreter/uberdata/"

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

sudo cp iuberdata /etc/init.d/iuberdata
sudo chmod +x /etc/init.d/iuberdata

# install Zeppelin
echo "downloading zeppelin"
sudo cp -rf /usr/share/zeppelin/notebook /tmp/
sudo cp -f /usr/share/zeppelin/conf/interpreter.json /tmp/notebook

sudo rm -rf /usr/share/zeppelin*

wget -c http://archive.apache.org/dist/zeppelin/zeppelin-$zeppelinVersion/zeppelin-$zeppelinVersion-bin-all.tgz -O /tmp/zeppelin-$zeppelinVersion.tgz

sudo tar -xzvf /tmp/zeppelin-$zeppelinVersion.tgz -C /usr/share
sudo mv /usr/share/zeppelin-$zeppelinVersion-bin-all /usr/share/zeppelin
sudo rm -f /tmp/zeppelin-*.tgz
#sudo find /usr/share -name zeppelin-* -type d -exec ln -s {} /usr/share/zeppelin \;
sudo rm -rf /usr/share/zeppelin/notebook/*

sudo mkdir -p $zeppelinInterpreterUberdataDir
cd $zeppelinInterpreterUberdataDir

sudo rm -rf ../spark
sudo bash /usr/share/zeppelin/bin/install-interpreter.sh --name spark --artifact org.apache.zeppelin:zeppelin-spark_2.10:0.6.1

sudo cp ../spark/zeppelin-spark*jar ./
sudo rm -rf ../spark
sudo rm -rf ../hive
sudo rm -rf ../tajo
sudo cp $SCRIPTPATH/zeppelin-site.xml /usr/share/zeppelin/conf/

sudo cp $SCRIPTPATH/interpreter.sh /usr/share/zeppelin/bin/
sudo chmod 777 /usr/share/zeppelin/bin/interpreter.sh

sudo ln -s /opt/spark/lib/spark-assembly* ./spark-assembly.jar
sudo ln -s /opt/spark/lib/datanucleus-core*.jar ./datanucleus-core.jar
sudo ln -s /opt/spark/lib/datanucleus-api*.jar ./datanucleus-api.jar
sudo ln -s /opt/spark/lib/datanucleus-rdbms*.jar ./datanucleus-rdbms.jar


cd $SCRIPTPATH
cd ../..

echo "clonning and building spark-timeseries v0.3.0"
git clone https://github.com/eleflow/spark-timeseries.git
cd spark-timeseries
git checkout holtwinters
mvn clean install -DskipTests -Dgpg.skip
sudo cp target/sparkts-0.3.0-jar-with-dependencies.jar $zeppelinInterpreterUberdataDir

cd $SCRIPTPATH
cd ../..

echo "clonning and building xgboost v0.6"
git clone --recursive https://github.com/eleflow/xgboost.git
cd xgboost
git checkout -b version6 v0.60
./build.sh
cd jvm-packages/
mvn clean install -DskipTests
sudo cp xgboost4j/target/xgboost4j-0.5-jar-with-dependencies.jar $zeppelinInterpreterUberdataDir
sudo cp xgboost4j-spark/target/xgboost4j-spark-0.5-jar-with-dependencies.jar $zeppelinInterpreterUberdataDir

cd $SCRIPTPATH
cd ../

echo "assembly iuberdata_zeppelin"
sbt "project iuberdata_zeppelin" assembly
echo "assembly iuberdata_addon_zeppelin"
sbt "project iuberdata_addon_zeppelin" assembly

cd $SCRIPTPATH

# install IUberdata
echo "preparing uberdata"
sudo cp -f ../iuberdata_zeppelin/target/scala-2.10/eleflow.uberdata.IUberdata-Zeppelin-0.1.0.jar $zeppelinInterpreterUberdataDir
sudo cp -f ../iuberdata_addon_zeppelin/target/scala-2.10/iuberdata_addon_zeppelin-assembly-0.1.0.jar /usr/share/zeppelin/lib/

sudo unzip notebook.zip -d /usr/share/zeppelin/notebook
sudo cp -f /usr/share/zeppelin/notebook/interpreter.json /usr/share/zeppelin/conf/

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
sudo chmod 777 -R /usr/share/zeppelin*
