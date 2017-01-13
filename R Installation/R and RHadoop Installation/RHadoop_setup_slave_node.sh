#On every node in the cluster
sudo apt-get update
sudo apt-get install r-base
echo export HADOOP_CMD=/usr/local/hadoop/bin/hadoop >> /home/hduser/.bashrc
echo export HADOOP_STREAMING=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar >> /home/hduser/.bashrc
echo export LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/lib/amd64:/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server >> /home/hduser/.bashrc
echo 'options(repos=structure(c(CRAN="http://ftp.iitm.ac.in/cran/")))' >> /etc/R/Rprofile.site
echo "Install R Cran R JAVA"
sudo apt-get install r-cran-rjava
R CMD javareconf
Rscript -e 'install.packages(c("rJava"))'
Rscript -e 'install.packages(c("functional"))'
sudo apt-get install r-cran-rcpp
sudo apt-get install r-cran-reshape2
Rscript -e 'install.packages(c("RJSONIO", "bitops", "digest", "stringr","dplyr","R.methodsS3", "caTools","Hmisc"))'
Rscript -e 'install.packages("/home/hduser/rmr2_3.3.1.tar.gz",repos=NULL,type="source")'
