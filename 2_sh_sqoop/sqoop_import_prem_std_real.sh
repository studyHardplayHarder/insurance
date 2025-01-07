/export/server/sqoop/bin/sqoop import \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--table prem_std_real \
--hive-table insurance_ods.prem_std_real \
--hive-import \
--hive-overwrite \
--fields-terminated-by '\t' \
-m 1