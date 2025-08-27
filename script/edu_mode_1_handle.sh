#!/bin/bash
export HIVE_HOME=/usr/bin/hive

if [ $# == 1 ]
    then
	dateStr=$1
    else
	dateStr=`date +'%Y-%m-%d'`
fi

sqlStr="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
set hive.exec.orc.compression.strategy=COMPRESSION;


insert into table itcast_dwd.visit_consult_dwd partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select
    wce.session_id,
    wce.sid,
    unix_timestamp(wce.create_time) as create_time,  
    wce.seo_source,
    wce.ip,
    wce.area,
    wce.msg_count,
    wce.origin_channel,
    wcte.referrer,
    wcte.from_url,
    wcte.landing_page_url,
    wcte.url_title,
    wcte.platform_description,
    wcte.other_params,
    wcte.history,
    substr(wce.create_time,12,2) as hourinfo,
    substr(wce.create_time,1,4) as yearinfo, 
    quarter(wce.create_time) as quarterinfo,
    substr(wce.create_time,6,2) as monthinfo,
    substr(wce.create_time,9,2) as dayinfo
from (select * from itcast_ods.web_chat_ems where starts_time='${dateStr}') wce join (select * from itcast_ods.web_chat_text_ems where start_time='${dateStr}') wcte   
    on wce.id = wcte.id;
"

${HIVE_HOME} -e "${sqlStr}" -S
