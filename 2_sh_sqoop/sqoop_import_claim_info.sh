export SQOOP_HOME=/export/server/sqoop-1.4.7.bin_hadoop-2.6.0
$SQOOP_HOME/bin/sqoop import \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--table claim_info \
--hive-table insurance_ods.claim_info \
--hive-import \
--hive-overwrite \
--fields-terminated-by '\t' \
--delete-target-dir \
-m 1


