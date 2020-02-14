SETUP PROCESS STEPS
Pre-requsite:
sudo timedatectl set-timezone Africa/Nairobi
confirm :-
   date
   ls -l /etc/localtime

1.Install git
2.Git Clone applications
3.Install virtualenv(install pip first)
  sudo yum install libcurl-devel
  sudo yum groupinstall "Development Tools"
  yum install gcc libffi-devel python-devel openssl-devel
  Install JHBuild (optional)
4.Create venv for apps
5.Install requirements
  -install requirements(pip install -r requirements.txt)
6.Configure webserver to serve apps
  sudo yum -y install httpd
  yum -y install mod_wsgi
  add virtualhost for app to apache conf
   yum install wget
***install amqplib fork for 0.9.1 amqp protocal(https://github.com/celery/py-amqp)
   wget https://pypi.python.org/packages/23/39/06bb8bd31e78962675f696498f7821f5dbd11aa0919c5a811d83a0e02609/amqp-2.1.4.tar.gz#md5=035a475e42ef4f431b4e0dca113434bd
   cd to extracted folder
   python setup.py install
   pip install python-jose

****
Switch api apps to live branch in use (scorepesa_mo_consumer(optmized) and scorepesa_queue_consumer(ntrigger))
*****

7.test
8.add apps backend to proxy
  -map the hostname of mysql gen 2 sock on /etc/hosts
  -add instance ip to mysql gen2 access control and save(Under SQL tab)
9.Test kannel injection

10 Setup sdp proxy scripts for backup incase cant insert to kannel(following steps)
11.Install mysql client on instance(client for connecting to mysql server/sock)
   yum install mysql
12. Install PHP minimum 5.6
   Install repo e.g for case of centos 7 use below rpm commands
   rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
   rpm -Uvh https://mirror.webtatic.com/yum/el7/webtatic-release.rpm
   sudo yum install -y php56w php56w-opcache php56w-xml php56w-mcrypt php56w-gd php56w-devel php56w-mysql php56w-intl php56w-mbstring

13. Install soap-xml extension(yum search php-soap)
   sudo yum install -y php56w-soap

14. Install bcmatch extension for use by RabbitMQ php lib
   sudo yum install -y php56w php56w-bcmath (yum search bcmath)
15. Setup SDP webservices(set php include path(sudo vim /etc/php.ini))
   include_path = "/usr/share/php/sendSMS"
16. sudo systemctl restart httpd
17. Install Redis if not installed by requirements.txt
    check if installed -> pip freeze | grep redis
        if installed should see similar: redis==*.*.*
