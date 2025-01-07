/export/server/sqoop/bin/sqoop import \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--table policy_client \
--hive-table insurance_ods.policy_client \
--hive-import \
--hive-overwrite \
--fields-terminated-by '\t' \
-m 1