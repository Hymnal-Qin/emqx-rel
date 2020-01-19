make
cd emqx-rel/_build/emqx/rel/emqx/bin
./emqx console


---

启动emqx

./bin/emqx start
检查运行状态

./bin/emqx_ctl status
停止emqx

./bin/emqx stop
放开防火墙访问端口

rpm安装：
安装依赖

yum -y install lksctp-tools
安装包

yum -y install emqttd-centos7-v2.3.11-1.el7.centos.x86_64.rpm
启动

systemctl start emqttd.service