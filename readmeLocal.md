# Uberdata
[![License][license-badge]][license-url]

The Uberdata project is fast way of getting a [Spark](http://spark.apache.org/) cluster up and running on [AWS](http://aws.amazon.com) or locally
## Requirements
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Java 1.8](http://www.oracle.com/technetwork/pt/java/javase/downloads/jdk8-downloads-2133151.html)
* [SBT](http://www.scala-sbt.org/0.13/docs/Setup.html)
* [Maven](https://maven.apache.org/install.html)

## Installation
Clone the project Uberdata
```
$ git clone https://github.com/eleflow/uberdata.git
```
Run script ```uberdata/ansible/setup_zeppelin_local.sh```
```
$ ./setup_zeppelin_local.sh
```
To run zeppelin, execute the script ```zeppelin.sh``` located in ```/usr/share/zeppelin/bin```
```
$ ./zeppelin.sh
```
Zeppelin will be running at http://localhost:8080/

## Uberdata Zeppelin Notebook on AWS
To build and run on AWS go [here](https://github.com/eleflow/uberdata/blob/master/readmeAws.md)
## Authors
[Eleflow](http://www.eleflow.com.br/)

[license-badge]: https://img.shields.io/badge/License-Apache%202-blue.svg?style=flat
[license-url]: https://github.com/eleflow/uberdata/blob/master/LICENSE

