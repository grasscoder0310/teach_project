#!/bin/bash

export SQOOP_HOME=/usr/bin/sqoop

if [ $# == 1 ]
   then
      dateStr=$1
   else
      dateStr=`date -d '-1 day' +'%Y-%m-%d'`
fi

dateNowStr=`date +'%Y-%m-%d'`

yearStr=`date -d ${dateStr} +'%Y'`
monthStr=`date -d ${dateStr} +'%m'`

jdbcUrl='jdbc:mysql://192.168.52.150:3306/nev'
username='root'
password='123456'
m='1'

${SQOOP_HOME} import \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "SELECT 
id,create_date_time,session_id,sid,create_time,seo_source,seo_keywords,ip,
AREA,country,province,city,origin_channel,USER AS user_match,manual_time,begin_time,end_time,
last_customer_msg_time_stamp,last_agent_msg_time_stamp,reply_msg_count,
msg_count,browser_name,os_info, '${dateNowStr}' AS starts_time  
FROM web_chat_ems_${yearStr}_${monthStr} WHERE create_time BETWEEN '${dateStr} 00:00:00' AND '${dateStr} 
23:59:59' and \$CONDITIONS" \
--hcatalog-database itcast_ods \
--hcatalog-table web_chat_ems \
-m ${m}

${SQOOP_HOME} import \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "SELECT 
        temp2.*, '${dateNowStr}' AS start_time
FROM (SELECT id FROM web_chat_ems_${yearStr}_${monthStr} WHERE create_time BETWEEN '${dateStr} 00:00:00' 
AND '${dateStr} 23:59:59') temp1
        JOIN web_chat_text_ems_${yearStr}_${monthStr} temp2 ON temp1.id = temp2.id where 1=1 and \$CONDITIONS" \
--hcatalog-database itcast_ods \
--hcatalog-table web_chat_text_ems \
-m ${m}
