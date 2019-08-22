# proxysql-zabbix

This repository contains scripts, config and template for Zabbix to monitor ProxySQL.
requires pymysql

example:

/srv/zabbix/agent/scripts/proxysql/proxysql.py get ping

/srv/zabbix/agent/scripts/proxysql/proxysql.py get cluster 'proxysql_servers'
