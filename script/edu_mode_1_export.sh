#!/bin/bash

export SQOOP_HOME=/usr/bin/sqoop

if [ $# == 1 ]
then
   TD_DATE=$1  
else
   TD_DATE=`date -d '-1 day' +'%Y-%m-%d'`
fi

TD_YEAR=`date -d ${TD_DATE} +%Y`

mysql -uroot -p123456 -h192.168.52.150 -P3306 -e "delete from scrm_bi.visit_dws where yearinfo='$TD_YEAR'; delete from scrm_bi.consult_dws where yearinfo='$TD_YEAR';"

jdbcUrl='jdbc:mysql://192.168.52.150:3306/scrm_bi?useUnicode=true&characterEncoding=utf-8'
username='root'
password='123456'

${SQOOP_HOME} export \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--table visit_dws \
--hcatalog-database itcast_dws \
--hcatalog-table visit_dws \
--hcatalog-partition-keys yearinfo \
--hcatalog-partition-values $TD_YEAR \
-m 1

${SQOOP_HOME} export \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--table consult_dws \
--hcatalog-database itcast_dws \
--hcatalog-table consult_dws \
--hcatalog-partition-keys yearinfo \
--hcatalog-partition-values $TD_YEAR \
-m 1

