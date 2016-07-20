#/bin/bash
service iuberdata-ubuntu stop

sudo apt-get --assume-yes install zip
sudo apt-get --assume-yes install unzip
sudo apt-get --assume-yes install awscli
mkdir ~/.aws
cp config ~/.aws
cp /tmp/ansible.zip ~
cd ~
#unzip ansible.zip
sudo cp iuberdata-ubuntu /etc/init.d/
sudo chmod +x /etc/init.d/iuberdata-ubuntu
./spark-jars-ubuntu.sh
./iuberdata-ubuntu.sh

cp ~/zeppelin-site.xml /usr/share/zeppelin/conf/

service iuberdata-ubuntu start
git clone https://bitbucket.org/uberdata/install.git
