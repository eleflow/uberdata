#!/bin/bash
version=1.5.2-bin-2.6.0-cdh-5.4.2
mkdir -p /opt/spark
mkdir -p /opt/spark/conf
chmod 777 /opt/spark/conf
wget -O /tmp/spark-$version.tgz https://s3-us-west-2.amazonaws.com/uberdata-public/spark/spark-$version.tgz
tar -xzf /tmp/spark-$version.tgz --strip-components 1 --wildcards --no-anchored 'spark-assembly*.jar'
tar -xzf /tmp/spark-$version.tgz --strip-components 1 --wildcards --no-anchored 'datanucleus*.jar'
tar -xzf /tmp/spark-$version.tgz --strip-components 1 --wildcards --no-anchored '*/python/*' -C /opt/spark

# remove to avoid conflict with zeppelin's slf4j
zip -d lib/spark-assembly-$version.jar /org/slf4j/impl/StaticLoggerBinder.class

rm -rf /opt/spark/lib
rm -rf /opt/spark/python
mv lib /opt/spark
mv python /opt/spark
