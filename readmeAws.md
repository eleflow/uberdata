# Uberdata Zeppelin Notebook on ASW
# Setup
To run zeppelin and create cluster on AWS you need:
 1. [To install ansible](http://docs.ansible.com/intro_installation.html#installing-the-control-machine)
 1. [To install boto](http://boto.readthedocs.org/en/latest/getting_started.html#installing-boto)
 1. [AWS access keys](http://aws.amazon.com/developers/access-keys)
 1. [To configure aws credentials in boto](http://boto.readthedocs.org/en/latest/getting_started.html#configuring-boto-credentials)
 1. [Create a AWS IAM role](http://docs.aws.amazon.com/IAM/latest/UserGuide/roles-creatingrole-service.html) named **dev-ops**
  with the policies below:
```JSON
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": "ec2:*",
        "Effect": "Allow",
        "Resource": "*"
    }
    ]
}
```
And create the policies **AllowS3forDevOps** and **AllowPassRoleforDevOps** as follow;:
#### AllowS3forDevOps
```
{       "Version": "2012-10-17",
         "Statement": [{         
	   		"Action": "s3:*",
		    "Effect": "Allow",           
			"Resource": "*"         
		}]
}
```
#### AllowPassRoleforDevOps
```
{       "Version": "2012-10-17",       
		 "Statement": [{           
		 	"Sid": "Stmt1409776891000",           
			"Effect": "Allow",           
			"Action": ["iam:PassRole"],
            "Resource": ["*"]         
		 }]     
}
```
Create a security group **IUberdataApplication** and configure some vars on script ```/uberdata/ansible/iuberdata-prov.yml```
```
    keypair: "keypairname"
    instance_type: "r3.xlarge"
    price: "0.15"
    image: "ami-e9527ed9"
    group: "IUberdataApplication"
    region: "us-west-2"
    zone: "us-west-2a"
    iamrole: "dev-ops"
```
Then run script  to set up a new aws instance
```
$ ansible-playbook -i inventory/local iuberdata-prov.yml
```
Run script iuberdata.yml to install and configure the new instance
```
$ ansible-playbook -i inventory/hosts iuberdata.yml
```
Zeppelin will run at new instance ip.

## Monitoring
### Ganglia

The master instance of your cluster also has a monitoring tool named Ganglia installed and it's address is displayed when you create the Context.
Ganglia is a useful tool that help you to monitor the CPU, memory and disk usage, displaying graphs of this components. JVM data like, gc executions. It's very useful to help you to setup the correct cluster size, for your tasks.
The ganglia address is printed in the screen during the cluster instantiation. It's always deployed to the masterhost:5080/ganglia address.
It's important to note that the information showed at ganglia has a little delay.