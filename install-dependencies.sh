#!/usr/bin/env bash
git clone https://github.com/sryza/spark-timeseries.git
cd spark-timeseries
mvn package install --quiet -DskipTests -Dgpg.skip