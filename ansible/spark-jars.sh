#!/bin/bash
buildVersion=1.6.2-bin-2.6.0-cdh5.4.2

mkdir -p /opt/spark
mkdir -p /opt/spark/conf
echo "donwloading spark"
wget https://uberdata-public.s3.amazonaws.com/spark/spark-$buildVersion.tgz -P /tmp

tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored 'spark-assembly*.jar'
tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored 'datanucleus*.jar'
tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/python/*' -C /opt/spark
tar -xzf /tmp/spark-$buildVersion.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/R/*' -C /opt/spark

# remove to avoid conflict with zeppelin's slf4j
zip -d lib/spark-assembly-*.jar /org/slf4j/impl/StaticLoggerBinder.class

rm -rf /opt/spark/lib
rm -rf /opt/spark/python
rm -rf /opt/spark/R
mv lib /opt/spark
mv python /opt/spark
mv R /opt/spark

rm -f /tmp/spark-$buildVersion.tgz
rm -rf /tmp/spark-$buildVersion