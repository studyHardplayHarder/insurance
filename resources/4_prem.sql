use insurance_ods;
set spark.sql.shuffle.partitions=8;
--Spark 2.x版本中默认不支持笛卡尔积操作，需要手动开启
set spark.sql.crossJoin.enabled=true;
--禁止精度损失
set spark.sql.decimalOperations.allowPrecisionLoss=false;
--创建一个性别表
drop VIEW if exists sex_table;
create or replace view sex_table as
select stack(2, 'M', 'F') sex;
select *
from sex_table;

--创建缴费期表
drop view if exists ppp_table;
create or replace view ppp_table as
select stack(4, 10, 15, 20, 30) ppp;
select *
from ppp_table;
--生成连续的序列
drop view if exists seq_table;
create or replace view seq_table as
select explode(sequence(1, 200)) id;
select *
from seq_table;
--生成固定参数表
drop view if exists input;
create or replace view input as
select 106    terminate_age,   -- 满期年龄(Terminate Age)
       0.035  interest_rate,   --预定利息率(Interest Rate PREM&RSV)
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

--生成所有的性别，缴费期，投保年龄组成的初始表
drop view if exists prem_src0;
create or replace  view prem_src0 as
select q.id            as age_buy,     --投保年龄,
       i.nursing_Age,--长期护理保险金给付期满年龄
       s.sex,                          --性别
       i.terminate_age    t_age,-- 满期年龄(Terminate Age)
       p.ppp,                          --缴费期
       106 - q.id      as bpp,         --保障期间
       i.interest_rate,                --预定利息率(Interest Rate PREM&RSV)
       i.sa,--基本保险金额(Baisc Sum Assured)
       y.id            as policy_year, --保单年度
       q.id + y.id - 1 as age          --某个保单年度的年龄
from sex_table s
         cross join ppp_table p on 1 = 1
--借助seq_table生成投保年龄
         join seq_table q on q.id >= 18 and q.id <= 70 - p.ppp
--借助seq_table生成保单年度
         join seq_table y on y.id >= 1 and y.id <= 106 - q.id
         join input i on 1 = 1;

--查看并验证prem_src0内容
select *
from prem_src0
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;

--3.3.1 步骤1 计算是否在缴费期内ppp_、bpp_字段
drop view if exists prem_src1;
create or replace  view prem_src1 as
select *,
       --case when policy_year<=ppp then 1
       --    else 0
       --end    as ppp_,--是否在缴费期间内 ppp_
       if(policy_year <= ppp, 1, 0) as ppp_,--是否在缴费期间内
       if(policy_year <= bpp, 1, 0) as bpp_--是否在保险期间内 bpp_
from prem_src0;
--查看并验证prem_src1内容
select age_buy, sex, ppp, policy_year, age, ppp_, bpp_
from prem_src1
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;
--3.3.2 步骤2 计算死亡率qx、kx、qx_ci字段
drop view if exists prem_src2;
create or replace  view prem_src2 as
select p.*,
       case
           when p.age <= 105 then
               if(p.sex = 'M', m.cl1, m.cl2)
           else 0
           end * i.MortRatio_Prem_0 * p.bpp_    as qx,--死亡率 qx
       case
           when p.age <= 105 then
               if(p.sex = 'M', d.k_male, d.k_female)
           else 0
           end * bpp_                           as kx,--残疾死亡占死亡的比例 kx
       if(p.sex = 'M', d.male, d.female) * bpp_ as qx_ci--残疾率 qx_ci
from prem_src1 p
         join input i on 1 = 1
         join insurance_ods.mort_10_13 m
              on m.age = p.age
         join insurance_ods.dd_table d
              on d.age = p.age
;

--3.3.3 步骤3 计算qx_d字段
drop view if exists prem_src3;
create or replace  view prem_src3 as
select *,
       if(age = 105, qx - qx_ci, qx * (1 - kx)) * bpp_ as qx_d --扣除残疾的死亡率
from prem_src2;

--步骤四（lx有效保单数）
-- 1、计算保单年度=1的有效保单数dx字段,其他年度置为null后续再算，注册成临时视图给下次用
create or replace view prem_src4_1 as
select *,
       if(policy_year = 1, 1, null) as lx --有效保单数
from prem_src3;
-- 2、依据上面保单年度=1的临时表，用udaf函数计算保单年度=2到最后的每年的lx字段。
create or replace temporary view prem_src4 as
select age_buy,
       nursing_Age,
       sex,
       t_age,
       ppp,
       bpp,
       interest_rate,
       sa,
       policy_year,
       age,
       ppp_,
       bpp_,
       qx,
       kx,
       qx_ci,
       qx_d,
       udf_lx(qx, lx) over (partition by age_buy,sex,ppp order by policy_year) lx
from prem_src4_1;

--校验结果跟Excel比对
select * from prem_src4 where age_buy=18 and ppp=10 and sex='M' order by policy_year;

--步骤五（健康人数dx_d、dx_ci、lx_d字段）
-- 1、计算保单年度=1的健康人数dx_d、dx_ci、lx_d字段,其他年度置为null后续再算，注册成临时视图给下次用
create or replace temporary view prem_src5_1 as
select *,
       if(policy_year = 1, 1, null)     as lx_d, --健康人数
       if(policy_year = 1, qx_d, null)  as dx_d,
       if(policy_year = 1, qx_ci, null) as dx_ci
from prem_src4;

-- 2、依据上面保单年度=1的临时表，用udaf函数计算保单年度=2到最后的每年的健康人数dx_d、dx_ci、lx_d字段。
create or replace temporary view prem_src5 as
with t1 as (
    select *,
           udf_lxd_dxd_dxci(lx_d, qx_d, qx_ci) over (partition by age_buy,sex,ppp order by policy_year) lx_d_qx_d_qx_ci
    from prem_src5_1
),
     t2 as (
         select *,
                split(lx_d_qx_d_qx_ci, '_') arr
         from t1
     )
select age_buy,
       nursing_Age,
       sex,
       t_age,
       ppp,
       bpp,
       interest_rate,
       sa,
       policy_year,
       age,
       ppp_,
       bpp_,
       qx,
       kx,
       qx_ci,
       qx_d,
       lx,
       cast(arr[0] as decimal(38, 16)) lx_d,
       cast(arr[1] as decimal(38, 16)) dx_d,
       cast(arr[2] as decimal(38, 16)) dx_ci
from t2;

--校验结果跟Excel比对
select * from prem_src5 where age_buy=18 and ppp=10 and sex='M' order by policy_year;

--步骤六、 当期发生该事件的概率，如下指的是死亡发生概率
create or replace temporary view prem_src6 as
select age_buy,
       nursing_Age,
       sex,
       t_age,
       ppp,
       bpp,
       interest_rate,
       sa,
       policy_year,
       age,
       ppp_,
       bpp_,
       qx,
       kx,
       qx_ci,
       qx_d,
       lx,
       lx_d,
       dx_d,
       dx_ci,
       dx_d / pow(1 + interest_rate, age + 1) as cx --当期发生该事件的概率
from prem_src5;
--验证
select *
from prem_src6
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;

--步骤7
create or replace temporary view prem_src7 as
select *,
       cx * pow(1 + interest_rate, 0.5)        as cx_,--对cx做调整，不精确的话，可以不做
       dx_ci / pow(1 + interest_rate, age + 1) as ci_cx--当期发生重疾的概率
from prem_src6;
--验证
select *
from prem_src7
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;

--步骤8
create or replace temporary view prem_src8 as
select *,
       ci_cx * pow(1 + interest_rate, 0.5) as ci_cx_, --当期发生重疾的概率，调整
       lx / pow(1 + interest_rate, age)    as dx,--有效保单生存因子 dx
       lx_d / pow(1 + interest_rate, age)  as dx_d_   --健康人数生存因子dx_d_
from prem_src7;
--验证
select *
from prem_src8
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;

--步骤9
create or replace temporary view prem_src9 as
select p.*,
       i.Nursing_Ratio,
    /*case
        when policy_year=1 then r.r1
        when policy_year=2 then r.r2
        when policy_year=3 then r.r3
        when policy_year=4 then r.r4
        when policy_year=5 then r.r5
        when policy_year>=6 then r.r6_
    end*ppp_    as expense,--附加费用率*/
       element_at(array(r1, r2, r3, r4, r5, r6_), least(policy_year, 6)) * ppp_ expense,   --附加费用率
       i.Disability_Ratio * p.bpp_                       as                     DB1,       --残疾给付
       if(p.age < i.Nursing_Age, 1, 0) * i.Nursing_Ratio as                     db2_factor --长期护理保险金给付因子 db2_factor
from prem_src8 p
         join insurance_ods.pre_add_exp_ratio r
              on p.ppp = r.ppp
         join input i on 1 = 1;
--验证
select *
from prem_src9
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;

--步骤10
create or replace temporary view prem_src10 as
select *,
       sum(dx * db2_factor) over (partition by age_buy,sex,ppp order by policy_year desc ) / dx as DB2, --长期护理保险金
       if(age >= nursing_age, 1, 0) * Nursing_Ratio                                             as DB3, --养老关爱金
       least(ppp, policy_year)                                                                  as DB4,--身故给付保险金
       sum(dx * ppp_)
           over (partition by age_buy,sex,ppp order by policy_year desc rows between unbounded preceding and 1 preceding)
           / dx * pow(1 + interest_rate, 0.5)                                                   as DB5  --豁免保费因子
from prem_src9;
--验证
select *
from prem_src10
where age_buy = 18
  and sex = 'M'
  and ppp = 10
order by policy_year;


--保存数据到hive表prem_src中
insert overwrite table insurance_dw.prem_src
select age_buy,
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
       expense,
       db1,
       db2_factor,
       db2,
       db3,
       db4,
       db5
from prem_src10;

--步骤11 聚合计算中间参数
create or replace  view prem_std1 as
select age_buy,
       sex,
       ppp,
       bpp,
       interest_rate,
       sa,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db1 * pow(1 + interest_rate, -0.25), ci_cx_ * db1)) as T11,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db2 * pow(1 + interest_rate, -0.25), ci_cx_ * db2)) as V11,
       sum(dx * db3)                                                                              as W11,
       sum(dx * ppp_)                                                                             as Q11,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate, 0.25)                   as T9,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate, 0.25)                   as V9,
       sum(dx * expense)                                                                          as S11,
       sum(cx_ * db4)                                                                             as X11,
       sum(ci_cx_ * db5)                                                                          as Y11
from insurance_dw.prem_src
group by age_buy, sex, ppp, bpp, interest_rate, sa
;
--验证
select * from prem_std1 where age_buy = 18  and sex = 'M'  and ppp = 10;


--步骤12 计算期交保费
create or replace temporary view prem_std2 as
select age_buy,
       sex,
       ppp,
       bpp,
       round(sa * (T11 + V11 + W11) / (Q11 - T9 - V9 - S11 - X11 - Y11), 0) as prem
from prem_std1;
--验证
select age_buy, sex, ppp, prem from prem_std2 order by ppp, sex desc, age_buy;

--如何做数据校验。
select my.age_buy,
       my.sex,
       my.ppp,
       my.prem           as          my_prem,
       he.prem           as          he_prem,
       my.prem - he.prem as          diff_prem,
       (my.prem - he.prem) / he.prem rete_diff
from prem_std2 my
         join insurance_ods.prem_std_real he
              on my.age_buy = he.age_buy
                  and my.sex = he.sex
                  and my.ppp = he.ppp
where (my.prem - he.prem) / he.prem > 0.002;

--保存到insurance_dw库
insert overwrite table insurance_dw.prem_std
select *
from prem_std2;
