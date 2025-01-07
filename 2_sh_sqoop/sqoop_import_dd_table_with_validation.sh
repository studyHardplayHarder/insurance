export SQOOP_HOME=/export/server/sqoop-1.4.7.bin_hadoop-2.6.0
$SQOOP_HOME/bin/sqoop import \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--table dd_table \
--hive-table insurance_ods.dd_table \
--hive-import \
--hive-overwrite \
--fields-terminated-by '\t' \
--delete-target-dir \
-m 1

#1、查询MySQL的表dd_table的条数
mysql_log=`$SQOOP_HOME/bin/sqoop eval \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--query "select count(1) from dd_table"
`
mysql_cnt=`echo $mysql_log | awk -F'|' {'print $4'} | awk {'print $1'}`
#2、查询hive的表dd_table的条数
hive_log=`hive -e "select count(1) from insurance_ods.dd_table"`

#3、比较2边的数字是否一样。
if [ $mysql_cnt -eq $hive_log ] ; then
echo "mysql表的数据量=$mysql_cnt,hive表的数据量=$hive_log,是相等的"
else
echo "mysql表的数据量=$mysql_cnt,hive表的数据量=$hive_log,不是相等的"
fi
