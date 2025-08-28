QOOP_HOME=/usr/bin/sqoop
HOST=192.168.52.150
USERNAME="root"
PASSWORD="123456"
PORT=3306
DBNAME="scrm_bi"
MYSQL=/usr/local/mysql_5723/bin/mysql
#上个月1日
if [[ $1 == "" ]];then
    Last_Month_DATE=$(date -d "-1 month" +%Y-%m-01)
else
    Last_Month_DATE=$1
fi
TD_YEAR=$(date -d "$Last_Month_DATE" +%Y)

${MYSQL} -h${HOST} -P${PORT} -u${USERNAME} -p${PASSWORD} -D${DBNAME} -e "delete from itcast_intention_app where yearinfo = '${Last_Month_DATE:0:4}'"
${SQOOP_HOME} export \
--connect "jdbc:mysql://${HOST}:${PORT}/${DBNAME}?useUnicode=true&characterEncoding=utf-8" \
--username ${USERNAME} \
--password ${PASSWORD} \
--table itcast_intention_app \
--hcatalog-database DWS \
--hcatalog-table itcast_intention_dws \
--hcatalog-partition-keys yearinfo  \
--hcatalog-partition-values ${TD_YEAR} \
-m 100

