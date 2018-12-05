在两台服务器间备份数据


1、下载
?wget https://download.samba.org/pub/rsync/src/rsync-3.1.2.tar.gz
?tar -zxvf rsync-3.1.2.tar.gz

2、安装
cd rsync-3.1.2
./configure --prefix=/usr/local/rsync
make
make install

3、配置rsyncd.conf
vim /usr/local/rsync/rsyncd.conf?


rsyncd.conf 的内容如下：


### 全局参数 ###
port=8730 # 【默认端口是873，这里改用8730了】
motd file=/usr/local/rsync/rsyncd.motd
log file=/usr/local/rsync/rsyncd.log
pid file=/var/run/rsyncd.pid


### 模块参数 ###
[testmodule]
path=/root/test
use chroot=true
uid=0
gid=0
read only=false
exclude=/readme.txt /runtime

#auth users后的用户名随便给，可以是系统中不存在的用户名
auth users=mazhenwei
secrets file = /usr/local/rsync/rsyncd.secrets

4、配置rsyncd.secrets

vim /usr/local/rsync/rsyncd.secrets

rsyncd.secrets的内容的语法为 用户名:登录密码

举例
mazhenwei:123

# rsyncd.secrets文件权限必须设置为600
chmod 600 /usr/local/rsync/rsyncd.secrets


5、配置rsyncd.motd
vim /usr/local/rsync/rsyncd.motd


rsyncd.motd的内容举例：
welcome use rsync service !


6、启动rsync服务
/usr/local/rsync/bin/rsync --daemon --config=/usr/local/rsync/rsyncd.conf


7、配置rsync开机启动：

vi /etc/rc.local

在末尾加上??/usr/local/rsync/bin/rsync --daemon --config=/usr/local/rsync/rsyncd.conf

推送目录/root/test/下的数据给b：

/usr/local/rsync/bin/rsync --port=8730 -av /root/test/ ?mazhenwei@10.0.87.2::testmodule --password-file=/root/mazhenwei.pass

拉取b的数据到本机/root/test/目录：

/usr/local/rsync/bin/rsync --port=8730 -av mazhenwei@10.0.87.21::testmodule ?/root/test/ ?--password-file=/root/mazhenwei.pass  ##这里的pass文件也要chmod  600
