# Uberdata
# Getting Started
## Requirements
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Java 1.8](http://www.oracle.com/technetwork/pt/java/javase/downloads/jdk8-downloads-2133151.html)
* [SBT](http://www.scala-sbt.org/0.13/docs/Setup.html)
* [Maven](https://maven.apache.org/install.html)
* [Zeppelin](http://www-us.apache.org/dist/zeppelin/zeppelin-0.6.0/zeppelin-0.6.0-bin-all.tgz)
* [Spark](http://www-us.apache.org/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz)
* [Spark-timeseries](https://github.com/sryza/spark-timeseries)

## Installation
Clone the project Uberdata
```
$ git clone [url]
```
Run script ```uberdata/ansible/setup_zeppelin_local.sh```
```
./setup_zeppelin_local.sh
```
To run zeppelin, execute the script ```zeppelin.sh``` located in ```zeppelin-0.6.0-bin-all/bin```
```
$ ./zeppelin.sh
```
Zeppelin will be running at http://localhost:8080/
## Uberdata Zeppelin Notebook on AWS
To build and run on AWS go [here](https://github.com/eleflow/uberdata/blob/master/readmeAws.md)
## Authors
[Eleflow](http://www.eleflow.com.br/)
## License
Apache - http://www.apache.org/licenses/LICENSE-2.0

