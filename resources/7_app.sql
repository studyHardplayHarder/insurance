use insurance_dw;
set spark.sql.shuffle.partitions=8;
--Spark 2.x版本中默认不支持笛卡尔积操作，需要手动开启
set spark.sql.crossJoin.enabled=true;
--计算产品的精算数据表
insert overwrite table insurance_app.policy_actuary
select a.age_buy,
       a.sex,
       a.ppp,
       a.bpp,
       a.policy_year,
       a.sa,
       a.cv_1a,
       a.cv_1b,
       a.sur_ben,
       a.np,
       coalesce(b.rsv2_re,0) as rsv2_re,
       b.rsv1_re,
       b.np_
from insurance_dw.cv_src a
join insurance_dw.rsv_src b
 --按照表的复合主键来关联
on a.policy_year=b.policy_year
and a.sex=b.sex
and a.age_buy=b.age_buy
and a.ppp=b.ppp;

--计算客户的精算信息表
set month='2021-08';
--1、用静态分区的方式插入数据，注意select的最后一列就不用是month字段了
--insert overwrite table insurance_app.policy_result partition (month=${month})
--2、用动态分区的方式插入数据，注意select的最后一列要是month字段
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
insert overwrite table insurance_app.policy_result partition (month)
select b.pol_no,
       b.user_id,
       c.name,
       c.sex,
       c.birthday,
       b.ppp,
       b.age_buy,
       b.buy_datetime,
       b.insur_name,
       b.insur_code,
       c.province,
       c.city,
       c.direction,
       a.bpp,
       a.policy_year,
       a.sa,
       a.cv_1a,
       a.cv_1b,
       a.sur_ben,
       a.np,
       a.rsv2_re,
       a.rsv1_re,
       a.np_,
       s.prem as prem_std, --原本每年应交多少保费
       if(a.policy_year<=b.ppp and month(${month})=month(b.buy_datetime),s.prem,0) as prem_thismonth,--本月应交保费
       ${month} as month
from insurance_ods.policy_client c
join insurance_ods.policy_benefit b
on c.user_id=b.user_id
join insurance_app.policy_actuary a
on a.age_buy=b.age_buy
and a.sex=c.sex
and a.ppp=b.ppp
and a.policy_year=ceil(months_between(${month},b.buy_datetime)/12)
join insurance_ods.prem_std_real s
on s.age_buy=b.age_buy
and s.sex=c.sex
and s.ppp=a.ppp;

select * from insurance_app.policy_result where month='2001-07';

--保费收入增长率
set month='2021-08';
select substr(add_months(${month},-1),0,7) as x;

cache table cache_app_agg_month_incre_rate as
with t1 as (
    --8月的保费收入
         select sum(prem_thismonth) prem from insurance_app.policy_result where month = ${month}),
     t2 as (
         --7月的保费收入
         select sum(prem_thismonth) last_prem from insurance_app.policy_result where month = substr(add_months(${month},-1),0,7))
select t1.prem,
       t2.last_prem,
       (t1.prem-t2.last_prem)/t2.last_prem as prem_incre_rate
from t1 left join  t2 on 1=1
;
insert overwrite table insurance_app.app_agg_month_incre_rate partition (month=${month})
select * from cache_app_agg_month_incre_rate;

--3.2 计算首年保费与保费收入比
insert overwrite table insurance_app.app_agg_month_first_of_total_prem partition (month =${month} )
select first_prem,
       total_prem,
       first_prem/total_prem as first_of_total_prem
  from
 (select
       sum(prem_std) as first_prem,
       sum((case
              --1、如果在缴费期内正常缴费，则取已经交过的所有保费。
               when ceil(months_between(${month},r.buy_datetime)/12)<=r.ppp and (b.elapse_date is null or b.elapse_date>${month} )  then
                   ceil(months_between(${month},r.buy_datetime)/12)
              --2、如果已经缴纳完毕，则取总体交过的所有保费。
               when ceil(months_between(${month},r.buy_datetime)/12)>r.ppp and (b.elapse_date is null or b.elapse_date>add_months(r.buy_datetime,cast(b.ppp*12 as int)) )  then
                   r.ppp
              --3、如果在缴费期内退保，取退保前缴纳的所有保费。
               when b.elapse_date < add_months(r.buy_datetime,cast(b.ppp*12 as int)) and b.elapse_date is not null  then
                  ceil(months_between(b.elapse_date,b.buy_datetime)/12)

           end)
           *prem_std) as total_prem
           --上面的case when 判断可以简化成下面的least
          --least(ceil(months_between(${month},r.buy_datetime)/12),r.ppp,ceil(months_between(b.elapse_date,b.buy_datetime)/12)),
from insurance_app.policy_result r
join insurance_ods.policy_benefit b
on b.pol_no=r.pol_no
where month=${month});

--3.3 个人营销渠道的件均保费
select insur_code,insur_name,
       sum(prem_thismonth)/count(t.pol_no) as prem_per_pol
from insurance_app.policy_result t
--新单条件
where  substr(t.buy_datetime,0,7)= '2001-07'
  and month='2001-07'
group by insur_code,insur_name;

--4.1 死亡发生率和残疾发生率
create or replace temporary  view app_agg_month_mort_dis_rate_view as
with t1 as (select r.insur_code, r.insur_name,
         ceil(months_between(c.claim_date,l.birthday)/12) as age,
    --发生了身故的保单数
         sum(if(c.claim_item like '%sgbxj%',1,0)) as sg_cnt,
         sum(if(c.claim_item like '%scbxj%',1,0)) as sc_cnt,
         count(c.pol_no) as cnt
from insurance_app.policy_result r
join insurance_ods.policy_client l
on r.user_id=l.user_id
left join insurance_ods.claim_info c
on r.pol_no=c.pol_no
where month=${month} group by r.insur_code, r.insur_name, ceil(months_between(c.claim_date,l.birthday)/12)
    ),
   t2 as (
    select insur_code,
    insur_name,
    age,
    sg_cnt,
    sc_cnt,
    sum(cnt) over (partition by insur_code,insur_name) as all_cnt
    from t1
    )
select insur_code,
       insur_name,
       age,
       --死亡发生率
       sg_cnt/all_cnt as sg_rate,
       sc_cnt/all_cnt as sc_rate
--残疾发生率
from t2
;
insert overwrite table insurance_app.app_agg_month_mort_dis_rate partition (month=${month})
select * from app_agg_month_mort_dis_rate_view;


--5.1 新业务价值率
insert overwrite table insurance_app.app_agg_month_nbev partition (month=${month})
select r.insur_code,r.insur_name,
       sum(r.prem_std*s.nbev)/sum(r.prem_std) as nbev
from insurance_app.policy_result r
join insurance_ods.prem_std_real s
on s.age_buy=r.age_buy
and s.sex=r.sex
and s.ppp=r.ppp
where month=${month}
group by r.insur_code,r.insur_name;

--
insert overwrite table insurance_app.app_agg_month_high_net_rate partition (month=${month})
select
       count(if(c.income>=10000000,1,null))/count(1) as high_net_rate
from  insurance_app.policy_result r
join insurance_ods.policy_client c
on r.user_id=c.user_id
where month=${month};

--
insert overwrite table insurance_app.app_agg_month_dir partition (month=${month})
select direction,
       count(1) as sum_users,
       sum(prem_thismonth) as sum_prem,
       sum(r.cv_1b) as sum_cv_1b,
       sum(sur_ben) as  sum_sur_ben,
       sum(rsv2_re) as sum_rsv2_re
from  insurance_app.policy_result r
where month=${month}
group by direction