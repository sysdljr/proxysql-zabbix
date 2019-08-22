# proxysql-zabbix

This repository contains scripts, config and template for Zabbix to monitor ProxySQL.
requires pymysql

example:
/etc/zabbix/scripts/proxysql.py get ping

/etc/zabbix/scripts/proxysql.py get cluster 'proxysql_servers'
