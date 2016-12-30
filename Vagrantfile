# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # enable env plugin
  #config.env.enable
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://atlas.hashicorp.com/search.
  config.vm.box = "centos/7"
  #config.vm.box_url = "file://", "https://atlas.hashicorp.com/centos/boxes/7/versions/1610.01/providers/virtualbox.box"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  #config.vm.network "private_network", ip: "192.168.50.4"

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # config.vm.network "forwarded_port", guest: 80, host: 8080
  # httpd
  # config.vm.network "forwarded_port", guest: 80, host: 4567

  # rabbit-mq
  config.vm.network "forwarded_port", guest: 15672, host: 15672
  # mongo
  config.vm.network "forwarded_port", guest: 27017, host: 27020
  # api
  config.vm.network "forwarded_port", guest: 5000, host: 5000
  # gulp/web
  config.vm.network "forwarded_port", guest: 3000, host: 3000

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  #config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  config.vm.synced_folder "../", "/vagrant", type: "rsync",
    rsync__exclude: [ "fo_web/node_modules" , "fo_notifications/node_modules" ]

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
  # config.vm.provider "virtualbox" do |vb|
  #   # Display the VirtualBox GUI when booting the machine
  #   vb.gui = true
  #
  #   # Customize the amount of memory on the VM:
  #   vb.memory = "1024"
  # end
  #
  # View the documentation for the provider you are using for more
  # information on available options.

  # Define a Vagrant Push strategy for pushing to Atlas. Other push strategies
  # such as FTP and Heroku are also available. See the documentation at
  # https://docs.vagrantup.com/v2/push/atlas.html for more information.
  # config.push.define "atlas" do |push|
  #   push.app = "YOUR_ATLAS_USERNAME/YOUR_APPLICATION_NAME"
  # end

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.
  
  # Erlang install documentation: http://www.jeramysingleton.com/install-erlang-and-elixir-on-centos-7-minimal/
  # http://www.linuxveda.com/2015/01/07/install-mongodb-linux/
  config.vm.provision "shell", inline: <<-SHELL
  	sudo yum -y install expect expectk
  SHELL
  config.vm.provision "shell", inline: <<-SHELL

    echo "---------------------------"
	echo "---- Overriding bashrc ----"
    echo "---------------------------"
    # override base .bashrc
    sudo cp -f /vagrant/fo_vagrant/.bashrc /home/vagrant
    source /home/vagrant/.bashrc

	echo " "
    echo "-----------------------"
	echo "---- Upgrading yum ----"
    echo "-----------------------"
	sudo yum -y update && sudo yum -y upgrade
	sudo yum -y install wxBase.x86_64

	echo " "
    echo "--------------------------"
    echo "---- Installing mongo ----"
    echo "--------------------------"
	if [ ! -e /etc/yum.repos.d/mongodb-org-3.2.repo ]; then
		echo "Creating Mongo 3.2 yum repo"
        sudo cp /vagrant/fo_vagrant/mongo/mongodb-org-3.2.repo /etc/yum.repos.d
	fi
	sudo yum install -y mongodb-org

    # override init.d/mongo configurations using mongo provided /etc/sysconfig/mongod
    sudo cp -f /vagrant/fo_vagrant/mongo/sysconfig-mongod /etc/sysconfig/mongod

	# create the necessary mongodb directories
	sudo mkdir -p /data/mongo/db
    sudo mkdir -p /data/mongo/log
    sudo mkdir -p /data/mongo/conf

    # copy the config files to
    sudo cp -f /vagrant/fo_vagrant/mongo/mongo*.cfg /data/mongo/conf

    # restart mongo unsecured
    sudo mongod --config=/data/mongo/conf/mongo.cfg

    # create the dev user
    echo "creating the mongo dev user"
    sudo mongo admin < /vagrant/fo_vagrant/mongo/mongo_create_user.js

    # stop and restart mongo secured
    echo "restarting mongo in secured mode"
    sudo mongod --shutdown --dbpath /data/mongo/db
    sudo mongod --config=/data/mongo/conf/mongo_secured.cfg

	echo " "
    echo "---------------------------"
	echo "---- Installing apache ----"
    echo "---------------------------"
	yum install -y httpd.x86_64
    # start apache
	sudo httpd

	echo " "
    echo "---------------------------"
	echo "---- Installing nodejs ----"
    echo "---------------------------"
	curl --silent --location https://rpm.nodesource.com/setup_6.x | bash -
  	yum install -y nodejs
	yum install -y wget.x86_64
	sudo yum install -y xdotool.x86_64

    sudo yum install -y epel-release

    echo "-----------------------------------------"
    echo "---- Installing helper packages ----"
    echo "-----------------------------------------"

    cd /home/vagrant

    # for rpy2
    yum install -y gcc
    yum install -y readline-devel

	echo " "
    echo "-----------------------------"
	echo "---- Installing RabbitMQ ----"
    echo "-----------------------------"
    cd /home/vagrant
	if [ ! -e /home/vagrant/rabbitmq-server-3.6.5-1.noarch.rpm ]; then
		rpm --import https://www.rabbitmq.com/rabbitmq-release-signing-key.asc
        if [ -e /vagrant/fo_vagrant/rabbitmq/rabbitmq-server-3.6.5-1.noarch.rpm ]; then
            sudo cp /vagrant/fo_vagrant/rabbitmq/rabbitmq-server-3.6.5-1.noarch.rpm /home/vagrant
        else
		    wget https://www.rabbitmq.com/releases/rabbitmq-server/v3.6.5/rabbitmq-server-3.6.5-1.noarch.rpm -P /home/vagrant
		fi
	fi
    sudo yum -y install rabbitmq-server-3.6.5-1.noarch.rpm

	if [ ! -e /etc/rabbitmq/rabbitmq.config ]; then
        sudo cp /vagrant/fo_vagrant/rabbitmq/rabbitmq.config /etc/rabbitmq
	fi
	if [ ! -e /etc/rabbitmq/enable_plugins ]; then
        sudo cp /vagrant/fo_vagrant/rabbitmq/enable_plugins /etc/rabbitmq
	fi

    # start rabbit
	sudo rabbitmq-server start -detached

    # enable rabbit plugins (ie: management console)
    sudo rabbitmq-plugins enable rabbitmq_management

    echo " "
    echo "-----------------------------"
    echo "---- Installing anaconda ----"
    echo "-----------------------------"
    anaconda_home=/home/vagrant
    if [ ! -d $anaconda_home/anaconda3 ]; then
        if [ ! -e $anaconda_home/Anaconda3-4.2.0-Linux-x86_64.sh ]; then
            if [ -e /vagrant/fo_vagrant/anaconda/Anaconda3-4.2.0-Linux-x86_64.sh ]; then
                sudo cp /vagrant/fo_vagrant/anaconda/Anaconda3-4.2.0-Linux-x86_64.sh $anaconda_home
            else
                wget https://repo.continuum.io/archive/Anaconda3-4.2.0-Linux-x86_64.sh -P $anaconda_home
            fi
            chmod +x $anaconda_home/Anaconda3-4.2.0-Linux-x86_64.sh
        fi

        cd /home/vagrant
        # install in batch mode which will automatically accept the license, etc
        ./Anaconda3-4.2.0-Linux-x86_64.sh -b -p $anaconda_home/anaconda3
    fi

    # the path is updated, but requires the user to restart their SSH session.
    # so we will need to be explicit in our calls to anaconda during this script
    conda_path=$anaconda_home/anaconda3/bin

    # create fo_api conda environment
    cd /vagrant/fo_api
    $conda_path/conda env create -f environment.yml

    # install notifications dependencies
    cd /vagrant/fo_notifications
    npm install

    # install web dependencies
    cd /vagrant/fo_web
    npm install
    sudo npm install -g gulp@3.9.0

    # create fo_worker conda environment
    cd /vagrant/fo_worker
    $conda_path/conda env create -f environment.yml

    # install R into the fo_worker conda env
    source $conda_path/activate fo_worker
    $conda_path/conda install --yes -c r r
    $conda_path/conda install --yes -c r r-essentials
    $conda_path/conda install --yes -c r rpy2
    $conda_path/conda install --yes -c r r-gdata
    $conda_path/conda install --yes -c bioconda r-pracma=1.8.8
    source $conda_path/deactivate fo_worker

    echo "--------------------------------"
    echo "---- Installing admin tools ----"
    echo "--------------------------------"
    # net-tools: provide commands such as ifconfig
    yum install -y net-tools

    echo "--------------------------------"
    echo "---- Installing app scripts ----"
    echo "--------------------------------"
    sudo cp /vagrant/fo_vagrant/apps/* /home/vagrant
    sudo chmod 755 /home/vagrant/*.sh

    echo "-------------------------------"
    echo "---- Base Install Complete ----"
    echo "-------------------------------"
	echo "Please visit README for more help."
  SHELL

  config.vm.provision "startup", type: "shell", run: "always", inline: <<-SHELL
    # start rabbit
    sudo rabbitmq-server start -detached
    # start mongo
    sudo mongod --config=/data/mongo/conf/mongo_secured.cfg
  SHELL

end
