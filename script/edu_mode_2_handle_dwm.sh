#! /bin/bash
SQOOP_HOME=/usr/bin/sqoop

#上个月1日
Last_Month_DATE=$(date -d "-1 month" +%Y-%m-01)
${HIVE_HOME} -S -e "
insert into table DWM.itcast_intention_dwm partition (yearinfo,monthinfo,dayinfo)
select
    dwd.customer_id,
    dwd.create_date_time,
    cus.area,
    dwd.itcast_school_id,
    sch.name as itcast_school_name,
    dwd.deleted,
    dwd.origin_type,
    dwd.itcast_subject_id,
    sub.name as itcast_subject_name,
    dwd.hourinfo,
    dwd.origin_type_stat,
    if(clue.clue_state='VALID_NEW_CLUES', '1', if(clue.clue_state='VALID_PUBLIC_NEW_CLUE', '0', '-1')) as clue_state_stat,
    e.department_id as tdepart_id,
    dept.name as tdepart_name,
    dwd.yearinfo,
    dwd.monthinfo,
    dwd.dayinfo
from DWD.itcast_intention_dwd dwd
left join itcast_ods.customer_clue clue on clue.customer_relationship_id=dwd.rid
left join itcast_dimen.customer cus on dwd.customer_id = cus.id
left join itcast_dimen.employee e on dwd.creator = e.id
left join itcast_dimen.scrm_department dept on e.department_id = dept.id
left join itcast_dimen.itcast_subject sub on dwd.itcast_subject_id = sub.id
left join itcast_dimen.itcast_school sch on dwd.itcast_school_id = sch.id
where concat_ws('-',dwd.yearinfo,dwd.monthinfo,dwd.dayinfo) >= '${Last_Month_DATE}';
"

