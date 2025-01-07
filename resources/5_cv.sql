use insurance_dw;
set spark.sql.shuffle.partitions=8;
--Spark 2.x版本中默认不支持笛卡尔积操作，需要手动开启
set spark.sql.crossJoin.enabled=true;
--禁止精度损失
set spark.sql.decimalOperations.allowPrecisionLoss=false;
--生成固定参数表
drop view if exists input;
create or replace  view input as
select 106    terminate_age, -- 满期年龄(Terminate Age)
       0.035  interest_rate,    --预定利息率(Interest Rate PREM&RSV)
       0.055  interest_rate_cv,--现金价值预定利息率（Interest Rate CV）
       0.0004 acci_qx,--意外身故死亡发生率(Accident_qx)
       0.115  rdr,--风险贴现率（Risk Discount Rate)
       10000  sa,--基本保险金额(Baisc Sum Assured)
       1      average_size,--平均规模(Average Size)
       1      MortRatio_Prem_0,--Mort Ratio(PREM)
       1      MortRatio_RSV_0,--Mort Ratio(RSV)
       1      MortRatio_CV_0,--Mort Ratio(CV)
       1      CI_RATIO,--CI Ratio
       6      B_time1_B,--生存金给付时间(1)—begain
       59     B_time1_T,--生存金给付时间(1)-terminate
       0.1    B_ratio_1,--生存金给付比例(1)
       60     B_time2_B,--生存金给付时间(2)-begain
       106    B_time2_T,--生存金给付时间(2)-terminate
       0.1    B_ratio_2,--生存金给付比例(2)
       70     MB_TIME,--祝寿金给付时间
       0.2    MB_Ration,--祝寿金给付比例
       0.7    RB_Per,--可分配盈余分配给客户的比例
       0.7    TB_Per,--未分配盈余分配给客户的比例
       1      Disability_Ratio,--残疾给付保险金保额倍数
       0.1    Nursing_Ratio,--长期护理保险金保额倍数
       75     Nursing_Age--长期护理保险金给付期满年龄
;
--2.4.1 步骤13 基于prem_src表继续计算
--因为步骤20 rt字段在policy_year=0时有意义，所以要union all上policy_year=0时的值。
create or replace view cv_src1 as
select age_buy,
       p.nursing_age,
       sex,
       t_age,
       ppp,
       bpp,
       p.interest_rate,
       p.sa,
       i.interest_rate_cv,
       policy_year,
       age,
       qx,-- 死亡率
       kx,--残疾死亡占死亡的比例
       qx_d,---扣除残疾的死亡率
       qx_ci,--残疾率
       dx_d,
       dx_ci,
       lx,--有效保单数
       lx_d,--健康人数
       dx_d/pow(1+i.interest_rate_cv,age+1) as cx,
       ppp_,--是否在缴费期间
       bpp_,--是否在保险期间
       expense,--附加费用率
       db1,--残疾给付
       db2_factor,--长期护理保险金给付因子
       db3,-- 养老关爱金
       db4--身故给付保险
from insurance_dw.prem_src p
join input i on 1=1
union all
select distinct
       age_buy,
       p.nursing_age,
       sex,
       t_age,
       ppp,
       bpp,
       p.interest_rate,
       p.sa,
       i.interest_rate_cv,
       0 as policy_year,
       null as age,
       null as qx,-- 死亡率
       null as kx,--残疾死亡占死亡的比例
       null as qx_d,---扣除残疾的死亡率
       null as qx_ci,--残疾率
       null as dx_d,
       null as dx_ci,
       null as lx,--有效保单数
       null as lx_d,--健康人数
       null as cx,
       null as ppp_,--是否在缴费期间
       null as bpp_,--是否在保险期间
       null as expense,--附加费用率
       null as db1,--残疾给付
       null as db2_factor,--长期护理保险金给付因子
       null as db3,-- 养老关爱金
       null as db4--身故给付保险
from insurance_dw.prem_src p
join input i on 1=1;



--步骤14 计算调整的死亡发生概率cx_和当期发生重疾的概率ci_cx
create or replace view cv_src2 as
select *,
       cx*pow(1+interest_rate_cv,0.5) as cx_,--调整的死亡发生概率
       dx_ci/pow(1+interest_rate_cv,age+1) as ci_cx--当期发生重疾的概率
from cv_src1;



--步骤15 计算ci_cx_ 、dx 、dx_d_字段
create or replace view cv_src3 as
select *,
       ci_cx*pow(1+interest_rate_cv,0.5) as ci_cx_,--当期发生重疾的概率，调整
       lx/pow(1+interest_rate_cv,age) as dx,--有效保单生存因子 dx
       lx_d/pow(1+interest_rate_cv,age) as dx_d_--健康人数生存因子dx_d_
from cv_src2;


--步骤16 计算db2、db5字段
create or replace view cv_src4 as
select *,
       sum(dx*db2_factor) over(partition by age_buy,sex,ppp order by policy_year desc )/dx as DB2,--长期护理保险金
       sum(dx*ppp_) over(partition by age_buy,sex,ppp order by policy_year rows between 1 following and unbounded following)/dx*pow(1+interest_rate_cv,0.5) as DB5--豁免保费因子
from cv_src3;



--步骤17 计算保单价值准备金毛保险费的9个参数
create or replace view prem_cv1 as
select age_buy,sex,ppp,sa,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db1 * pow(1 + interest_rate_cv, -0.25), ci_cx_ * db1)) as T11,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db2 * pow(1 + interest_rate_cv, -0.25), ci_cx_ * db2)) as V11,
       sum(dx * db3)                                                                              as W11,
       sum(dx * ppp_)                                                                             as Q11,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate_cv, 0.25)                   as T9,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate_cv, 0.25)                   as V9,
       sum(dx * expense)                                                                          as S11,
       sum(cx_ * db4)                                                                             as X11,
       sum(ci_cx_ * db5)                                                                          as Y11
       from cv_src4
group by age_buy,sex,ppp,interest_rate_cv,sa
;
select *
from prem_cv1
where age_buy=18 and sex ='M' and ppp=10;
--步骤18 计算保单价值准备金毛保险费prem_cv
create or replace view prem_cv2 as
select p.age_buy,p.sex,p.ppp,
        (SA*(T11+V11+W11)+s.PREM*(T9+V9+X11+Y11))/(Q11-S11) as prem_cv --保单价值准备金毛保险费
from prem_cv1 p
join insurance_ods.prem_std_real s
on s.age_buy=p.age_buy
and s.sex=p.sex
and s.ppp=p.ppp;

select *
from prem_cv2
where age_buy=18 and sex ='M' and ppp=10;

--对所有的prem_cv结果做比对
select a.age_buy,
       a.sex,
       a.ppp,
       a.prem_cv my_prem_cv,
       b.prem_cv his_prem_cv,
       a.prem_cv-b.prem_cv as diff_prem_cv,
       (a.prem_cv-b.prem_cv)/b.prem_cv as rate_diff
from prem_cv2 a
join insurance_ods.prem_cv_real b
on a.age_buy=b.age_buy
and a.sex=b.sex
and a.ppp=b.ppp
where abs((a.prem_cv-b.prem_cv)/b.prem_cv)>0.003;

insert overwrite table insurance_dw.prem_cv
select * from prem_cv2;

--步骤19 计算净保费np_、pvnp、pvdb1~5字段
create or replace view cv_src5 as
select a.*,
       (ppp_-expense)*b.prem_cv as  np_,--净保费 np_
       b.prem_cv*sum(dx*(ppp_-expense)) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx pvnp,--净保费现值

       if( policy_year=1,
           (sa*sum(ci_cx_*db1) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year rows between 1 following and unbounded following)
                +0.5*
                 (c.prem*ci_cx_*pow(1+interest_rate_cv,0.25)+sa*db1*ci_cx_*pow(1+interest_rate_cv,-0.25) ))/dx,
           sa* sum(ci_cx_*db1) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx
           )   as pvdb1,
       if( policy_year=1,
           (sa*sum(ci_cx_*db2) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year rows between 1 following and unbounded following)
               +0.5*
                (c.prem*ci_cx_*pow(1+interest_rate_cv,0.25)+sa*db2*ci_cx_*pow(1+interest_rate_cv,-0.25) ))/dx,
           sa* sum(ci_cx_*db2) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx
           ) as pvdb2,
        sa*sum(dx*db3) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx as pvdb3,
        c.prem*sum(cx_*db4) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx as pvdb4,
       c.prem*sum(ci_cx_*db5) over(partition by a.age_buy,a.sex,a.ppp order by a.policy_year desc )/dx as pvdb5
from cv_src4 a
join insurance_ods.prem_cv_real b
     on a.age_buy=b.age_buy
         and a.sex=b.sex
         and a.ppp=b.ppp
join insurance_ods.prem_std_real c
     on a.age_buy=c.age_buy
         and a.sex=c.sex
         and a.ppp=c.ppp
;

--验证
select * from cv_src5
where age_buy=18 and sex ='M' and ppp=10
order by policy_year;

--步骤20 保单价值准备金pvr、rt字段
create or replace view cv_src6 as
select *,
       if(policy_year=0,null,
           lead(pvdb1+pvdb2+pvdb3+pvdb4+pvdb5-pvnp) over (partition by age_buy,sex,ppp order by policy_year)
           ) pvr, --保单价值准备金 pvr
       case when ppp=1 then 1
           else if(policy_year>=least(20,ppp),1,0.8+policy_year*0.8/least(20,ppp))
       end    rt
from cv_src5;

--验证
select * from cv_src6
where age_buy=18 and sex ='M' and ppp=10
order by policy_year;

--2.4.9 步骤21 计算修匀净保费np、生存金sur_ben、cv_1b字段
create or replace view cv_src7 as
select *,
       np_ * lag(rt) over (partition by age_buy,sex,ppp order by policy_year) as                       NP,--修匀净保费
       db3 * sa                                                               as                       sur_ben,--生存金
       rt * greatest(pvr - lead(db3 * sa) over (partition by age_buy,sex,ppp order by policy_year), 0) cv_1b--现金价值年末（生存给付后）
from cv_src6;

--验证
select * from cv_src7
where age_buy=18 and sex ='M' and ppp=10
order by policy_year;

--步骤22 计算现金价值年末（生存给付前）cv_1a

create or replace view cv_src8 as
select *,
       cv_1b+lead(sur_ben) over (partition by age_buy,sex,ppp order by policy_year) cv_1a--现金价值年末（生存给付前）cv_1a
  from cv_src7;
--步骤23 计算现金价值年中cv_2
create or replace view cv_src9 as
select *,
       (np+lag(cv_1b) over (partition by age_buy,sex,ppp order by policy_year)+cv_src8.cv_1a)/2 cv_2--现金价值年中
 from cv_src8;

--验证
select * from cv_src9
where age_buy=18 and sex ='M' and ppp=10
order by policy_year;

-- 保存数据到DW结果表cv_src
insert overwrite table insurance_dw.cv_src
select
    age_buy,
    nursing_age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate_cv,
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
    expense,
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
    pvr,
    rt,
    np,
    sur_ben,
    cv_1a,
    cv_1b,
    cv_2
from cv_src9;

select * from insurance_dw.cv_src
where age_buy=18 and sex ='M' and ppp=10
order by policy_year;
