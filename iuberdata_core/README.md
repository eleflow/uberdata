# Spark Notebook

[![Build Status][build-badge]][build-url]
[![License][license-badge]][license-url]

The Spark Notebook project is fast way of getting a [Spark](http://spark.apache.org/) cluster up and running on [AWS](http://aws.amazon.com) with the friendly [IPython](http://ipython.org) interface.

## Before you start
You'll need 

1. to have [Docker installed](https://docs.docker.com/installation/) (recommended) or [no docker setup](nodocker.md)
1. [AWS access keys](http://aws.amazon.com/developers/access-keys) 
1. One [AWS keypair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair)

 
## Setup
1. git clone https://github.com/eleflow/sparknotebook.git
1. cd sparknotebook
1. create a aws.deploy.env file with these:

  ```sh
  AWS_ACCESS_KEY_ID=<YOUR AWS ACCESS KEY>
  AWS_SECRET_ACCESS_KEY=<YOUR AWS SEECRET ACCESS KEY>
  AWS_KEY_PAIR=<YOUR AWS KEY PAIR NAME>
  ```
1. Run 

``` $ docker build --rm -f=aws.deploy.Dockerfile -t=aws.deploy .```

## Running the Notebook on AWS

1. Run `sudo docker run -it --env-file ./aws.deploy.env --volume $PWD:/sparknotebook --volume $HOME/.ssh:/.ssh aws.deploy` and if all goes well you will see the ip of your sparknotebook server in a line like this
  ```sh 
  ...

  PLAY RECAP ******************************************************************** 
  52.10.183.42               : ok=21   changed=3    unreachable=0    failed=0   
  ```
1. Where 52.10.183.42 will be replaced with another ip address. Put that ip address on your browser to get access to the notebook

## Spark Notebook

Spark Notebook kernel is deployed into your server, and you can access it through the port 80, using an HTTP browser.
The initial notebook state is showed in the picture below:

![Alt text](/../images/images/EmptyNotebook.png?raw=true "Initial state of a Spark Notebook")

To start a new notebook, just click in the New Notebook button, and you will be redirected to a new tab, containing an empty notebook.
The notebook is a code container that contains multiple TextArea components, where you can insert any kind of Scala code, including multi lines scripts. To execute the desired code, put the focus into the code TextArea component and hit Shift + ENTER or click in the play button (positioned at the notebook Header). Each time that you submit a code to the notebook, it will be compiled and if it compiles, it will be executed.

## Cluster Settings

One of the cluster settings you are likely to change is the number of slaves. To change it to 30, you can run this code on the Spark Noteook
```scala
  ClusterSettings.coreInstanceCount = 30 // Number of workers available in your cluster - default to 3
```
To see other settings see  [ClusterSettings](src/main/scala/eleflow/sparknotebook/SparkNotebookContext.scala)
## SparkContext
A SparkContext can be accessed with:
```scala
  sparkContext
```
This is a method of SparkNotebookContext and it provisions the machines and sets up the cluster the first time it runs. An example of output of this method is showed below:

![Alt text](/../images/images/ClusterInstantiation.png?raw=true "Sample output of a cluster instantiation")

## Shutdown

To shutdown the cluster and terminate the cluster master and slaves run:
```scala
    terminate
```

## Monitoring
### Ganglia

The master instance of your cluster also has a monitoring tool named Ganglia installed and it's address is displayed when you create the SparkContext.
Ganglia is a useful tool that help you to monitor the CPU, memory and disk usage, displaying graphs of this components. JVM data like, gc executions. It's very useful to help you to setup the correct cluster size, for your tasks.
The ganglia address is printed in the screen during the cluster instantiation. It's always deployed to the masterhost:5080/ganglia address.
It's important to note that the information showed at ganglia has a little delay.

# Local build

To build and run locally go [here](buildlocal.md)

# License

This project is distributed under Apache License Version 2.0

[build-badge]: https://travis-ci.org/eleflow/sparknotebook.svg?branch=master
[build-url]: https://travis-ci.org/eleflow/sparknotebook
[license-badge]: https://img.shields.io/badge/License-Apache%202-blue.svg?style=flat
[license-url]: LICENSE
