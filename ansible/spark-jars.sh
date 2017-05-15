#!/bin/bash
buildVersion=2.1.0-bin-2.6.0-cdh5.4.2

rm -rf /opt/spark
rm -rf /tmp/spark-$buildVersion.tgz*

mkdir -p /opt/spark
mkdir -p /opt/spark/conf
echo "donwloading spark"
wget -c https://uberdata-public.s3.amazonaws.com/spark/spark-$buildVersion.tgz -P /tmp

tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*jars/*'
tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/python/*' -C /opt/spark
tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/R/*' -C /opt/spark

# remove to avoid conflict with zeppelin's slf4j
rm -f slf4j*.jar


rm -rf /opt/spark/lib
rm -rf /opt/spark/jars
rm -rf /opt/spark/python
rm -rf /opt/spark/R
mv jars /opt/spark
mv python /opt/spark
mv R /opt/spark


rm -f /tmp/spark-$buildVersion.tgz
rm -rf /tmp/spark-$buildVersion
