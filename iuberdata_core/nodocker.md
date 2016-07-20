# Setup without Docker

If you want to use Sparknotebook without docker you have to:
 1. [To install ansible](http://docs.ansible.com/intro_installation.html#installing-the-control-machine)
 1. [To install boto](http://boto.readthedocs.org/en/latest/getting_started.html#installing-boto)
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
