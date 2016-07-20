FROM ubuntu:14.04
MAINTAINER Paulo Magalhaes

RUN apt-get update && apt-get install -y \
    python \
    python-pip \
	software-properties-common \
	&& apt-add-repository ppa:ansible/ansible \
	&& apt-get update && apt-get install -y ansible

RUN pip install awscli
RUN pip install boto
ADD aws.deploy.env /tmp/aws.deploy.env
RUN . /tmp/aws.deploy.env && printf "[defaults]\nprivate_key_file=/.ssh/${AWS_KEY_PAIR}.pem\nhost_key_checking=False" > ~/.ansible.cfg

ENTRYPOINT /sparknotebook/aws.deploy.sh
