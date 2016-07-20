#!/usr/bin/env bash

zeppelinVersion="0.6.0"
sparkVersion="1.6.2"

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

cd $SCRIPTPATH
cd ../..

#download Zeppelin
wget "http://www-us.apache.org/dist/zeppelin/zeppelin-"$zeppelinVersion"/zeppelin-"$zeppelinVersion"-bin-all.tgz"
tar -xzvf "zeppelin-"$zeppelinVersion"-bin-all.tgz"

#download Spark
wget "http://www-us.apache.org/dist/spark/spark-"$sparkVersion"/spark-$sparkVersion.tgz"
tar -xzvf "spark-"$sparkVersion".tgz"

ZEPPELIN_HOME="$(pwd)/zeppelin-"$zeppelinVersion"-bin-all"
SPARK_HOME="$(pwd)/spark-"$sparkVersion

#build Spark to hadoop 2.6, hive, sparkr and thrift
bash $SPARK_HOME/make-distribution.sh --skip-java-test -Psparkr -Phadoop-2.6 -Phive -Phive-thriftserver -Dhadoop.version=2.6.0-cdh5.4.2

mkdir $ZEPPELIN_HOME"/interpreter/uberdata"

#create links to interpreter
ln -s $SPARK_HOME"/dist/lib/datanucleus-api-jdo-3.2.6.jar" $ZEPPELIN_HOME"/interpreter/uberdata"
ln -s $SPARK_HOME"/dist/lib/datanucleus-core-3.2.10.jar" $ZEPPELIN_HOME"/interpreter/uberdata"
ln -s $SPARK_HOME"/dist/lib/datanucleus-rdbms-3.2.9.jar" $ZEPPELIN_HOME"/interpreter/uberdata"
ln -s $SPARK_HOME"/dist/lib/spark-assembly-"$sparkVersion"-hadoop2.6.0-"*".jar" $ZEPPELIN_HOME"/interpreter/uberdata"

cp -f $ZEPPELIN_HOME"/interpreter/spark/zeppelin-spark-"$zeppelinVersion".jar" $ZEPPELIN_HOME"/interpreter/uberdata"
rm -rf $ZEPPELIN_HOME"/interpreter/spark"

#clone and build spark-timeseries, a uberdata dependency
git clone https://github.com/sryza/spark-timeseries.git
cd spark-timeseries
mvn package install -DskipTests -Dgpg.skip

cd $SCRIPTPATH
cd ../

UBERDATA_HOME="$(pwd)"

#assembly uberdata interpreter to zeppelin
sbt "project iuberdata_zeppelin" assembly

sudo cp -f $UBERDATA_HOME"/ansible/zeppelin-site.xml" $ZEPPELIN_HOME"/conf"
sudo cp -f $UBERDATA_HOME"/ansible/interpreter.sh" $ZEPPELIN_HOME"/bin"
sudo chmod +x $ZEPPELIN_HOME"/bin/interpreter.sh"
sudo unzip $UBERDATA_HOME"/ansible/shell.zip" -d $ZEPPELIN_HOME"/notebook"

sudo cp $UBERDATA_HOME"/iuberdata_zeppelin/target/scala-2.10/eleflow.uberdata.IUberdata-Zeppelin-0.1.0.jar" $ZEPPELIN_HOME"/interpreter/uberdata"