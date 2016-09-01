#!/usr/bin/env bash
echo "clonning and building sparkts"
git clone https://github.com/sryza/spark-timeseries.git
cd spark-timeseries
mvn package install --quiet -DskipTests -Dgpg.skip

cd ../
echo "clonning and building xgboost"
git clone https://github.com/dmlc/xgboost
cd xgboost/jvm-packages/
mvn package install -DskipTests