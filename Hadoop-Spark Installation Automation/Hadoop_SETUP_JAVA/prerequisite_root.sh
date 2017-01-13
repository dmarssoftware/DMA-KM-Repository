#sudo passwd root
#Install OpenSSH in the machine
#Install SSHPass in the machine
#Reboot the machine

cd
sudo apt-get update
echo "Installation of OpenSSH Server"
sudo apt install openssh-server
echo "Backing up ssh config file"
sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.factory-defaults
sudo chmod a-w /etc/ssh/sshd_config.factory-defaults
echo "Changing Root Login permission in config file"
sed 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config > /etc/ssh/tmp_sshdconfig
mv /etc/ssh/tmp_sshdconfig /etc/ssh/sshd_config
echo "Installation of SSHPass"
sudo apt-get install sshpass
echo "Downloading the root_setup script from SVN"
wget --user admin --password Admin123 -O /home/hadoop/root_setup.sh http://172.18.100.188/svn/svn_repo/Hadoop_setup_Java/root_setup.sh
echo "Downloading the MN_hadoop_script from SVN"
wget --user admin --password Admin123 -O /home/hadoop/MN_hadoop_script.sh http://172.18.100.188/svn/svn_repo/Hadoop_setup_Java/MN_hadoop_script.sh
chmod 777 /home/hadoop/root_setup.sh
chmod 777 /home/hadoop/MN_hadoop_script.sh
echo "System will go for a reboot"
sleep 5
reboot
