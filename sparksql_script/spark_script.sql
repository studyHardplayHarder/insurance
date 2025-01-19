-- select 1,2,3,4,5;
select explode(array(1,2,3,4,5));
select explode(split('1,2,3,4,5', ','));
select explode(sequence(to_date('2024-10-31'),to_date('2025-01-13'), interval 1 month));
--stack: 平均分成n行
select stack(2,1,2,3,4,5,6);
--构建表
-- drop table  if exists t3;
-- create table t3
--     as select stack(2,1,2,3,4,5,6);
-- select * from t3;
--构建临时试图
-- create or replace temporary view t4 as
--  select stack(2,1,2,3,4,5,6);
--构建永久试图
create or replace view t5 as
    select stack(2,'男','M','女','F');
select * from t5;
--缓存表
cache table t6 as select stack(2,'男','M','女','F');
select * from t6;
--视图和表的区别：试图保存的是sql语句，表保存的是数据到磁盘。试图-中间文件，表-结果
with c1 as(
select explode(`array`(1,2,3)) as col1
)
select col1, col1+2 from c1;

create or replace view t2(c1, c2, c3) as values
(1,3,23),
(1,4,8),
(1,5,4),
(1,6,3),
(2,1,15),
(2,2,5),
(2,3,17),
(2,4,9);
select * from t2;
--纵向if c2=1,c4=1, else c4=(上一个c4+当前c3)/2
select
    c1,
    c2,
    c3,
    if(c2=1,1, null)  as c4
from t2;

-----******************