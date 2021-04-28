#!/usr/bin/env bash

sudo yum install -y cmake

if [! -f /home/ec2-user/xgboost/jvm-packages/xgboost4j/target/xgboost4j-0.7-jar-with-dependencies.jar]; then
cd /tmp
wget https://cmake.org/files/v3.10/cmake-3.10.0.tar.gz
tar -xvzf cmake-3.10.0.tar.gz
cd cmake-3.10.0
sudo ./bootstrap
sudo make
sudo make install

sudo wget http://mirror.nbtelecom.com.br/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.zip -O /opt/apache-maven-3.3.9-bin.zip
cd /opt
sudo unzip -f apache-maven-3.3.9-bin.zip
export PATH=$PATH:/opt/apache-maven-3.3.9/bin

cd /home/ec2-user
git clone --recursive https://github.com/dmlc/xgboost
cd xgboost
git checkout -b version7 v0.7
./build.sh
cd jvm-packages/
mvn clean install -DskipTests
fi
cp /home/ec2-user/xgboost/jvm-packages/xgboost4j/target/xgboost4j-0.7-jar-with-dependencies.jar /tmp/xgboost4j-0.7-jar-with-dependencies.jar
cp /home/ec2-user/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.7-jar-with-dependencies.jar /tmp/xgboost4j-spark-0.7-jar-with-dependencies.jar
cp /home/ec2-user/xgboost/jvm-packages/xgboost4j/target/xgboost4j-0.7-jar-with-dependencies.jar /usr/share/zeppelin/interpreter/uberdata/
cp /home/ec2-user/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.7-jar-with-dependencies.jar /usr/share/zeppelin/interpreter/uberdata/
