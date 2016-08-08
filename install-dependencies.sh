#!/usr/bin/env bash
git clone https://github.com/sryza/spark-timeseries.git
cd spark-timeseries
sudo mvn package install -DskipTests -Dgpg.skip