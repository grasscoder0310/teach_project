#!/bin/bash

export SQOOP_HOME=/usr/bin/sqoop

if [ $# == 1 ]
    then
	dateStr=$1
    else
	dateStr=`date -d '-1 day' + '%Y-%m-%d'`
fi

dateNowStr=`date + '%Y-%m-%d'`
monthStr=`date -d ${dateStr} + '%Y'`

jdbcUrl='jdbc:mysql://192.168.52.150:3306/nev'
username='root'
password='123456'
m='1'

${SQOOP_HOME} import \
--connect ${jdbcUrl} \
--username ${username} \
--password ${password} \
--query "select
create_date_time,
       update_date_time,
       deleted,
       customer_id,
       first_id,
       belonger,
       belonger_name,
       initial_belonger,
       distribution_handler,
       business_scrm_department_id,
       last_visit_time,
       next_visit_time,
       origin_type,
       itcast_school_id,
       itcast_subject_id,
       intention_study_type,
       anticipat_signup_date,
       level,
       creator,
       current_creator,
       creator_name,
       origin_channel,
       comment,
       first_customer_clue_id,
       last_customer_clue_id,
       process_state,
       process_time,
       payment_state,
       payment_time,
       signup_state,
       signup_time,
       notice_state,
       notice_time,
       lock_state,
       lock_time,
       itcast_clazz_id,
       itcast_clazz_time,
       payment_url,
       payment_url_time,
       ems_student_id,
       delete_reason,
       deleter,
       deleter_name,
       delete_time,
       course_id,
       course_name,
       delete_comment,
       close_state,
       close_time,
       appeal_id,
       tenant,
       total_fee,
       belonged,
       belonged_time,
       belonger_time,
       transfer,
       transfer_time,
       follow_type,
       transfer_bxg_oa_account,
       transfer_bxg_belonger_name,
       FROM_UNIXTIME(unix_timestamp(), "%Y-%m-%d") as start_time,
       date_format("9999-12-31", "%Y-%m-%d")       as end_time
from customer_relationship_update where 1=1 and \$conditions" \
--hcatalog-database ODS \
--hcatalog-table customer_relationship_update \
--m ${m}
