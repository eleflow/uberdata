#!/bin/bash
version=1.6.2-bin-2.6.0-cdh5.4.2

mkdir -p /opt/spark
mkdir -p /opt/spark/conf

wget http://www-us.apache.org/dist/spark/spark-1.6.2/spark-1.6.2.tgz -O /tmp
tar -xzvf /tmp/spark-$version.tgz -C /tmp
#OPÇÃO SKIP
bash /tmp/spark-$version/make-distribution.sh --tgz --skip-java-test -Psparkr -Phadoop-2.6 -Phive -Phive-thriftserver -Dhadoop.version=2.6.0-cdh5.4.2

tar -xzf /tmp/spark-$version.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored 'spark-assembly*.jar'
tar -xzf /tmp/spark-$version.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored 'datanucleus*.jar'
tar -xzf /tmp/spark-$version.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/python/*' -C /opt/spark
tar -xzf /tmp/spark-$version.tgz --no-same-owner --strip-components 1 --wildcards --no-anchored '*/R/*' -C /opt/spark

# remove to avoid conflict with zeppelin's slf4j
zip -d lib/spark-assembly-*.jar /org/slf4j/impl/StaticLoggerBinder.class

rm -rf /opt/spark/lib
rm -rf /opt/spark/python
rm -rf /opt/spark/R
mv lib /opt/spark
mv python /opt/spark
mv R /opt/spark

rm -f spark-1.6.2.tgz
rm -rf spark-1.6.2