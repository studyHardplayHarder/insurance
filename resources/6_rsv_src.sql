use insurance_dw;
set spark.sql.shuffle.partitions=8;
create or replace temporary view rsv_src_temp as
with t1 as (select p.age_buy,
                   nursing_age,
                   p.sex,
                   t_age,
                   p.ppp,
                   p.bpp,
                   interest_rate,
                   sa,
                   policy_year,
                   age,
                   qx,--死亡率
                   kx, --残疾死亡占死亡的比例
                   qx_d,--扣除残疾的死亡率
                   qx_ci, --残疾率
                   dx_d,
                   dx_ci,
                   lx,--有效保单数
                   lx_d,--健康人数
                   cx,
                   cx_,
                   ci_cx,
                   ci_cx_,
                   dx,
                   dx_d_,
                   ppp_,--是否在缴费期间
                   bpp_,--是否在保险期间
                   if(policy_year = 1,
                      0.5 * (sa * (p.db1) * pow(1 + interest_rate, -0.25) + prem * pow(1 + interest_rate, 0.25)),
                      sa * (db1)) as db1,--残疾给付
                   db2_factor,--长期护理保险金给付因子
                   if(policy_year = 1,
                      0.5 * (sa * (p.db2) * pow(1 + interest_rate, -0.25) + prem * pow(1 + interest_rate, 0.25)),
                      sa * (db2)) as db2,
                   sa * p.db3     as db3,-- 养老关爱金
                   s.prem * p.db4 as db4,--身故给付保险
                   s.prem * p.db5 as db5,
                   s.prem
            from prem_src                    p
            join insurance_ods.prem_std_real s on p.age_buy = s.age_buy and p.sex = s.sex and p.ppp = s.ppp),
     t2 as (select *,
                   sum(ci_cx_ * db1) over (partition by age_buy,sex,ppp order by policy_year desc) / dx as pvdb1,
                   sum(ci_cx_ * db2) over (partition by age_buy,sex,ppp order by policy_year desc) / dx as pvdb2,
                   sum(dx * db3) over (partition by age_buy,sex,ppp order by policy_year desc) / dx     as pvdb3,
                   sum(cx_ * db4) over (partition by age_buy,sex,ppp order by policy_year desc) / dx    as pvdb4,
                   sum(ci_cx_ * db5) over (partition by age_buy,sex,ppp order by policy_year desc) / dx as pvdb5
            from t1),
     t3 AS (select *,
                   sum(if(policy_year = 1, pvdb1 + pvdb2 + pvdb3 + pvdb4 + pvdb5, 0))
                       over (partition by age_buy,sex,ppp) / sum(dx * ppp_) over (partition by age_buy,sex,ppp) *
                   sum(if(policy_year = 1, dx, 0)) over (partition by age_buy,sex,ppp)      as Prem_rsv
            from t2),
     t4 as (
         select *,
                if(ppp=1,
                    Prem_rsv,
                   sum(if(policy_year = 1,
                          ((db1+  db2+  db5)* ci_cx_+
                              db3 * dx+ cx_ * db4)/ dx
                       , 0))
                       over (partition by age_buy,sex,ppp)
                    ) as alpha --修正纯保费首年 alpha
         from t3
     ),
     t5 as (
         select *,
                if(ppp=1,0,
                    prem_rsv+(Prem_rsv-alpha)/sum(if(policy_year>=2,dx * ppp_,0))over(partition by age_buy,sex,ppp) *
                    sum(if(policy_year=1,dx,0))over(partition by age_buy,sex,ppp)
                    ) as beta --修正纯保费续年 beta
         from t4
     ),
     t6 as (
         select *,
                if(policy_year=1,alpha,least(prem,beta))*ppp_ as np_---修正纯保费 np_
         from t5
     ),
     t7 as (
         select *,
                sum(dx*np_)over(partition by age_buy,sex,ppp order by policy_year desc)/dx as PVNP---修正纯保费现值
         from t6
     ),
     t8 as (
         select *,
                lead(pvdb1+pvdb2+pvdb3+pvdb4+pvdb5-pvnp) over (partition by age_buy,sex,ppp order by policy_year) as rsv1  ---准备金年末
         from t7
     ),
     t9 as (

         select * ,
                lag(rsv1) over (partition by age_buy,sex,ppp order by policy_year) as  rsv2---准备金年初（未加当年初纯保费）
          from t8
     ),
     t10 as (
         select a.*,
                greatest(rsv1,b.cv_1a) as rsv1_re,---修正责任准备金年末
                greatest(rsv2,lag(cv_1b) over (partition by a.age_buy,a.sex,a.ppp order by a.policy_year)) as rsv2_re--修正责任准备金年初(未加当年初纯保费）
         from t9 a
         join insurance_dw.cv_src  b
         on a.age_buy=b.age_buy
         and a.sex=b.sex
         and a.ppp = b.ppp
         and a.policy_year=b.policy_year
     )
select *
from t10
--where age_buy = 18  and sex = 'M'  and ppp = 10 order by policy_year
;

insert overwrite table insurance_dw.rsv_src
select
    age_buy,
    nursing_age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate,
    sa,
    policy_year,
    age,
    qx,
    kx,
    qx_d,
    qx_ci,
    dx_d,
    dx_ci,
    lx,
    lx_d,
    cx,
    cx_,
    ci_cx,
    ci_cx_,
    dx,
    dx_d_,
    ppp_,
    bpp_,
    db1,
    db2_factor,
    db2,
    db3,
    db4,
    db5,
    np_,
    pvnp,
    pvdb1,
    pvdb2,
    pvdb3,
    pvdb4,
    pvdb5,
    prem_rsv,
    alpha,
    beta,
    rsv1,
    rsv2,
    rsv1_re,
    rsv2_re
from rsv_src_temp;
