echo \# R Packages >> /etc/apt/sources.list
echo deb http://ftp.iitm.ac.in/cran/bin/linux/ubuntu trusty/ >> /etc/apt/sources.list
sudo apt-get update
sudo apt-get install r-base
sudo apt-get install gdebi-core
wget https://download2.rstudio.org/rstudio-server-0.99.903-amd64.deb
sudo gdebi rstudio-server-0.99.903-amd64.deb
echo export HADOOP_CMD=/usr/local/hadoop/bin/hadoop >> /home/hduser/.bashrc
echo export HADOOP_STREAMING=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar >> /home/hduser/.bashrc
echo export LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/lib/amd64:/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server >> /home/hduser/.bashrc
echo 'options(repos=structure(c(CRAN="http://ftp.iitm.ac.in/cran/")))' >> /etc/R/Rprofile.site
sudo rstudio-server verify-installation
echo "Install R Cran R JAVA"
sudo apt-get install r-cran-rjava
R CMD javareconf
echo "Running R from terminal"
Rscript -e 'install.packages(c("rJava"))'
Rscript -e 'install.packages(c("functional"))'
sudo apt-get install r-cran-rcpp
sudo apt-get install r-cran-reshape2
Rscript -e 'install.packages(c("RJSONIO", "bitops", "digest", "stringr","dplyr","R.methodsS3", "caTools","Hmisc"))'
#echo "Installing RHbase"
#sudo apt-get install libboost-dev libboost-test-dev libboost-program-options-dev libboost-system-dev libboost-filesystem-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev
#cd /usr/local/
#wget http://mirror.fibergrid.in/apache/thrift/0.9.3/thrift-0.9.3.tar.gz
#tar -zxvf thrift-0.9.3.tar.gz
#mkdir thrift
#mv thrift-0.9.3/* thrift/
#rm -rf thrift-0.9.3*
#cd thrift/
#./configure
#make
#sudo make install
#cd /usr/local/
#chown -R hduser:hadoop thrift/
#ln -s /usr/local/lib/libthrift-0.9.3.so /usr/lib/
#echo export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig >> /home/hduser/.bashrc



#Login to hduser account
#scp .bashrc hadoop-slave-1:/home/hduser/
#scp .bashrc hadoop-slave-2:/home/hduser/


#Transfer the rmr package to every node in the cluster
#scp rmr2_3.3.1.tar.gz hadoop-slave-1:/home/hduser/
#scp rmr2_3.3.1.tar.gz hadoop-slave-2:/home/hduser/

# On every node in the cluster
#wget https://github.com/RevolutionAnalytics/rmr2/releases/download/3.3.1/rmr2_3.3.1.tar.gz
#Rscript -e 'install.packages("/home/hduser/rmr2_3.3.1.tar.gz",repos=NULL,type="source")'
# On the node that will run the R Client
#wget https://github.com/RevolutionAnalytics/rhdfs/raw/master/build/rhdfs_1.0.8.tar.gz
#Rscript -e 'install.packages("/home/hduser/rhdfs_1.0.8.tar.gz",repos=NULL,type="source")'
