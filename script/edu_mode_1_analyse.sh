#!/bin/bash
export HIVE_HOME=/usr/bin/hive

if [ $# == 1 ]
then
   dateStr=$1
   else 
      dateStr=`date -d '-1 day' +'%Y-%m-%d'`
fi

yearStr=`date -d ${dateStr} +'%Y'`
monthStr=`date -d ${dateStr} +'%m'`

month_for_quarter=`date -d ${dateStr} +'%-m'`
quarterStr=$((($month_for_quarter-1)/3+1))

dayStr=`date -d ${dateStr} +'%d'`

sqlStr="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
set hive.exec.orc.compression.strategy=COMPRESSION;

alter table itcast_dws.visit_dws drop partition(yearinfo='${yearStr}',quarterinfo='-1',monthinfo='-1',dayinfo='-1');
alter table itcast_dws.visit_dws drop partition(yearinfo='${yearStr}',quarterinfo='${quarterStr}',monthinfo='-1',dayinfo='-1');
alter table itcast_dws.visit_dws drop partition(yearinfo='${yearStr}',quarterinfo='${quarterStr}',monthinfo='${monthStr}}',dayinfo='-1');

alter table itcast_dws.consult_dws drop partition(yearinfo='${yearStr}',quarterinfo='-1',monthinfo='-1',dayinfo='-1');
alter table itcast_dws.consult_dws drop partition(yearinfo='${yearStr}',quarterinfo='${quarterStr}',monthinfo='-1',dayinfo='-1');
alter table itcast_dws.consult_dws drop partition(yearinfo='${yearStr}',quarterinfo='${quarterStr}',monthinfo='${monthStr}}',dayinfo='-1');

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}'
group by area, yearinfo;
    
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by area, yearinfo,quarterinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by area, yearinfo,quarterinfo,monthinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by area, yearinfo,quarterinfo,monthinfo,dayinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,'-',hourinfo) as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by area, yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '2' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}'
group by seo_source, yearinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '-1' as from_url,
    '2' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by seo_source, yearinfo,quarterinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '-1' as from_url,
    '2' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by seo_source, yearinfo,quarterinfo,monthinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '-1' as from_url,
    '2' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'and dayinfo='${dayStr}'
group by seo_source, yearinfo,quarterinfo,monthinfo,dayinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,'-',hourinfo) as time_str,
    '-1' as from_url,
    '2' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by seo_source, yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '3' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}'
group by origin_channel, yearinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '-1' as from_url,
    '3' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by origin_channel, yearinfo,quarterinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '-1' as from_url,
    '3' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by origin_channel, yearinfo,quarterinfo,monthinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '-1' as from_url,
    '3' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by origin_channel, yearinfo,quarterinfo,monthinfo,dayinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,'-',hourinfo) as time_str,
    '-1' as from_url,
    '3' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by origin_channel, yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    from_url,
    '4' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}'
group by from_url, yearinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    from_url,
    '4' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by from_url, yearinfo,quarterinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    from_url,
    '4' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by from_url, yearinfo,quarterinfo,monthinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    from_url,
    '4' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by from_url, yearinfo,quarterinfo,monthinfo,dayinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,'-',hourinfo) as time_str,
    from_url,
    '4' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by from_url, yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}'
group by from_url, yearinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by from_url, yearinfo,quarterinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by from_url, yearinfo,quarterinfo,monthinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by from_url, yearinfo,quarterinfo,monthinfo,dayinfo;

insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select 
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1' as area,
    '-1' as seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,'-',hourinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from itcast_dwd.visit_consult_dwd where yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by from_url, yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;



insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '1' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}'
group by yearinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '1' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by yearinfo,quarterinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '1' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by yearinfo,quarterinfo,monthinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '1' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by yearinfo,quarterinfo,monthinfo,dayinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,' ',hourinfo) as time_str,
    '1' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    yearinfo as time_str,
    '2' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}'
group by yearinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'_',quarterinfo) as time_str,
    '2' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}'
group by yearinfo,quarterinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '2' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}'
group by yearinfo,quarterinfo,monthinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    '-1' as hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '2' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by yearinfo,quarterinfo,monthinfo,dayinfo,area;

insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,' ',hourinfo) as time_str,
    '2' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd where msg_count >= 1 and yearinfo='${yearStr}' and quarterinfo='${quarterStr}' and monthinfo='${monthStr}' and dayinfo='${dayStr}'
group by yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,area;
"

${HIVE_HOME} -e "${sqlStr}" -S
