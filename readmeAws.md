# Uberdata Zeppelin Notebook on AWS
# Setup
To run zeppelin and create cluster on AWS you need:
 1. [AWS access keys and .pem file](http://aws.amazon.com/developers/access-keys)
 1. A user with the policies: AmazonEC2FullAccess, IAMFullAccess and AmazonS3FullAccess 
 1. [Install ansible and configure .ansible.cfg file ](http://docs.ansible.com/intro_installation.html#installing-the-control-machine)
 1. [Install boto](http://boto.readthedocs.org/en/latest/getting_started.html#installing-boto) and [configure AWS credentials in .boto file](http://boto.readthedocs.org/en/latest/getting_started.html#configuring-boto-credentials)
 1. [Install awscli](https://aws.amazon.com/cli/)
 1. Run the script ```/uberdata/ansible/setup_zeppelin_local.sh```

Configure some vars on script ```/uberdata/ansible/iuberdata-prov.yml```
```
    keypair: "yourkeypairname"
    instance_type: "r3.xlarge"
    price: "0.15"
    image: "ami-e9527ed9"
    group: "IUberdataApplication"
    region: "us-west-2"
    zone: "us-west-2a"
    iamrole: "dev-ops"
```
Then run script to set up a new aws instance configure it:
```
$ ./uberdata/ansible/setup_zeppelin_aws.sh
```
Zeppelin will run at new instance ip.

## XGBoost/Time Series
There is an extra step needed to install XGBoost/Sparkts jars. To do this you have to execute the
 ansible/build-xgboost-light.sh script into the driver server, this script is responsible for 
 compiling xgboost in your server platform, and to copy the generated jars to the Zeppelin folder.
  
## Monitoring
### Ganglia

The master instance of your cluster also has a monitoring tool named Ganglia installed and it's address is displayed when you create the Context.
Ganglia is a useful tool that help you to monitor the CPU, memory and disk usage, displaying graphs of this components. JVM data like, gc executions. It's very useful to help you to setup the correct cluster size, for your tasks.
The ganglia address is printed in the screen during the cluster instantiation. It's always deployed to the masterhost:5080/ganglia address.
It's important to note that the information showed at ganglia has a little delay.
