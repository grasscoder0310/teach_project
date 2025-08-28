#!/bin/bash

export HIVE_HOME=/usr/bin/hive

Last_DATE=$(date -d "-1 day" +%Y-%m-%d)

${HIVE_HOME} -S -e 
"
insert into table DWD.itcast_intention_dwd partition(yearinfo,monthinfo,dayinfo)
select  
    id as rid,
    customer_id,
    create_date_time,
    if(itcast_school_id is null OR itcast_school_id = 0 , '-1',itcast_school_id) as itcast_school_id, 
    deleted,
    origin_type,
    if(itcast_subject_id is not null, if(itcast_subject_id != 0,itcast_subject_id,'-1'),'-1') as itcast_subject_id, 
    creator,
    substr(create_date_time,12,2) as hourinfo, 
    if(origin_type in('NETSERVICE','PRESIGNUP'),'1','0') as origin_type_stat,
    substr(create_date_time,1,4) as yearinfo,
    substr(create_date_time,6,2) as monthinfo,
    substr(create_date_time,9,2) as dayinfo 
from ODS.customer_relationship rs
where rs.deleted = 0
    and substr(rs.start_time, 1, 10) = '${Last_DATE}'
    and rs.end_time = '9999-12-31';
"
