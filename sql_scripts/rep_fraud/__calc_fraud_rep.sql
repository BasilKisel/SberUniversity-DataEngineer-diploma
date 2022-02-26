-- insert into DE1M.KISL_REP_FRAUD
-- (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)

select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       evnt.EVENT_TYPE,
       current_date
-- select count(*)
from DE1M.KISL_DWH_FACT_TRANSACTIONS tran
         left join DE1M.KISL_DWH_DIM_CARDS_HIST card
                   on tran.CARD_NUM = card.CARD_NUM
                       and tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
                       and card.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
                   on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
                       and tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
                       and acc.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
                   on acc.CLIENT = client.CLIENT_ID
                       and tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
                       and client.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_TERMINALS_HIST term
                   on term.TERMINAL_ID = tran.TERMINAL
                       and tran.TRANS_DATE between term.EFFECTIVE_FROM and term.EFFECTIVE_TO
                       and term.DELETED_FLG = 'N'
         left join (select 1 as EVENT_TYPE from dual) first_type
                   on (client.PASSPORT_VALID_TO + interval '1' day < tran.TRANS_DATE
                       or exists(select * -- To not duplicate transactions due to duplicates in passports
                                 from DE1M.KISL_DWH_FACT_PSSPRT_BLCKLST ps_bl
                                 where ps_bl.PASSPORT_NUM = client.PASSPORT_NUM)
                       )
         left join (select 2 as EVENT_TYPE from dual) second_type
                   on (tran.TRANS_DATE > acc.VALID_TO + interval '1' day)
         left join (select 3 as EVENT_TYPE from dual) third_type
                   on exists(select *
                             from DE1M.KISL_DWH_FACT_TRANSACTIONS oth_tran
                                      left join DE1M.KISL_DWH_DIM_TERMINALS_HIST oth_term
                                                on oth_term.TERMINAL_ID = oth_tran.TERMINAL
                                                    and
                                                   oth_tran.TRANS_DATE between oth_term.EFFECTIVE_FROM and oth_term.EFFECTIVE_TO
                                                    and oth_term.DELETED_FLG = 'N'
                             where tran.CARD_NUM = oth_tran.CARD_NUM
                               and oth_tran.TRANS_DATE between tran.TRANS_DATE - interval '1' hour and tran.TRANS_DATE
                               and oth_term.TERMINAL_CITY != term.TERMINAL_CITY
                       )
         left join (select fraud_tran.TRANS_ID, 4 as EVENT_TYPE
                    from (
                             select fraud_tran.*
                                  , lag(fraud_tran.OPER_TYPE, 1)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_OPER_TYPE
                                  , lag(fraud_tran.OPER_RESULT, 1)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_OPER_RESULT
                                  , lag(fraud_tran.TRANS_DATE, 1)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_TRANS_DATE
                                  , lag(fraud_tran.AMT, 1)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_AMT

                                  , lag(fraud_tran.OPER_TYPE, 2)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_OPER_TYPE
                                  , lag(fraud_tran.OPER_RESULT, 2)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_OPER_RESULT
                                  , lag(fraud_tran.TRANS_DATE, 2)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_TRANS_DATE
                                  , lag(fraud_tran.AMT, 2)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_AMT

                                  , lag(fraud_tran.OPER_TYPE, 3)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_OPER_TYPE
                                  , lag(fraud_tran.OPER_RESULT, 3)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_OPER_RESULT
                                  , lag(fraud_tran.TRANS_DATE, 3)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_TRANS_DATE
                                  , lag(fraud_tran.AMT, 3)
                                        over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_AMT
                             from DE1M.KISL_DWH_FACT_TRANSACTIONS fraud_tran
                         ) fraud_tran
                    where fraud_tran.TRANS_ID is not null
                      and fraud_tran.OPER_RESULT = 'SUCCESS'
                      and fraud_tran.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
                      and THIRD_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
                      and THIRD_TRY_OPER_RESULT = 'REJECT'
                      and THIRD_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
                      and THIRD_TRY_TRANS_DATE < fraud_tran.TRANS_DATE
                      and THIRD_TRY_AMT > fraud_tran.AMT
                      and SECOND_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
                      and SECOND_TRY_OPER_RESULT = 'REJECT'
                      and SECOND_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
                      and SECOND_TRY_TRANS_DATE < THIRD_TRY_TRANS_DATE
                      and SECOND_TRY_AMT > THIRD_TRY_AMT
                      and FIRST_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
                      and FIRST_TRY_OPER_RESULT = 'REJECT'
                      and FIRST_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
                      and FIRST_TRY_TRANS_DATE < SECOND_TRY_TRANS_DATE
                      and FIRST_TRY_AMT > SECOND_TRY_AMT) forth_type
                   on forth_type.TRANS_ID = tran.TRANS_ID
         inner join (
    select 1 as EVENT_TYPE
    from dual
    union all
    select 2 as EVENT_TYPE
    from dual
    union all
    select 3 as EVENT_TYPE
    from dual
    union all
    select 4 as EVENT_TYPE
    from dual
) evnt
                    on evnt.EVENT_TYPE in
                       (first_type.EVENT_TYPE, second_type.EVENT_TYPE, third_type.EVENT_TYPE, forth_type.EVENT_TYPE)
;

-- 1. Совершение операции при просроченном или заблокированном паспорте.
insert into DE1M.KISL_REP_FRAUD -- 1,801 rows affected in 362 ms
(EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       1,
       current_date
-- select count(*) -- 1801
from DE1M.KISL_DWH_FACT_TRANSACTIONS tran
         left join DE1M.KISL_DWH_DIM_CARDS_HIST card
                   on tran.CARD_NUM = card.CARD_NUM
                       and tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
                       and card.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
                   on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
                       and tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
                       and acc.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
                   on acc.CLIENT = client.CLIENT_ID
                       and tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
                       and client.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_FACT_PSSPRT_BLCKLST ps_bl
                   on ps_bl.PASSPORT_NUM = client.PASSPORT_NUM
where (ps_bl.PASSPORT_NUM is not null
    or (client.PASSPORT_VALID_TO + interval '1' day < tran.TRANS_DATE))
;

-- 2. Совершение операции при недействующем договоре.
insert into DE1M.KISL_REP_FRAUD -- 328 rows affected in 110 ms
(EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       2,
       current_date
-- select count(*) -- 328
from DE1M.KISL_DWH_FACT_TRANSACTIONS tran
         left join DE1M.KISL_DWH_DIM_CARDS_HIST card
                   on tran.CARD_NUM = card.CARD_NUM
                       and tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
                       and card.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
                   on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
                       and tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
                       and acc.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
                   on acc.CLIENT = client.CLIENT_ID
                       and tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
                       and client.DELETED_FLG = 'N'
where tran.TRANS_DATE > acc.VALID_TO + interval '1' day
;


-- 3. Совершение операций в разных городах в течение одного часа.
insert into DE1M.KISL_REP_FRAUD -- 10 rows affected in 15 s 982 ms
(EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       3,
       current_date
-- select count(*) -- 10
from DE1M.KISL_DWH_FACT_TRANSACTIONS tran
         left join DE1M.KISL_DWH_DIM_CARDS_HIST card
                   on tran.CARD_NUM = card.CARD_NUM
                       and tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
                       and card.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
                   on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
                       and tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
                       and acc.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
                   on acc.CLIENT = client.CLIENT_ID
                       and tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
                       and client.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_TERMINALS_HIST term
                   on term.TERMINAL_ID = tran.TERMINAL
                       and tran.TRANS_DATE between term.EFFECTIVE_FROM and term.EFFECTIVE_TO
                       and term.DELETED_FLG = 'N'
where exists(select *
             from DE1M.KISL_DWH_FACT_TRANSACTIONS oth_tran
                      left join DE1M.KISL_DWH_DIM_TERMINALS_HIST oth_term
                                on oth_term.TERMINAL_ID = oth_tran.TERMINAL
                                    and oth_tran.TRANS_DATE between oth_term.EFFECTIVE_FROM and oth_term.EFFECTIVE_TO
                                    and oth_term.DELETED_FLG = 'N'
             where tran.CARD_NUM = oth_tran.CARD_NUM
               and oth_tran.TRANS_DATE between tran.TRANS_DATE - interval '1' hour and tran.TRANS_DATE
               and oth_term.TERMINAL_CITY != term.TERMINAL_CITY
          );

-- select client.CLIENT_ID, client.PASSPORT_NUM, tran.CARD_NUM, term.TERMINAL_CITY, tran.TRANS_DATE, term.*
--  from DE1M.KISL_DWH_FACT_TRANSACTIONS tran
--          left join DE1M.KISL_DWH_DIM_CARDS_HIST card
--                    on tran.CARD_NUM = card.CARD_NUM
--                        and tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
--                        and card.DELETED_FLG = 'N'
--          left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
--                    on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
--                        and tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
--                        and acc.DELETED_FLG = 'N'
--          left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
--                    on acc.CLIENT = client.CLIENT_ID
--                        and tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
--                        and client.DELETED_FLG = 'N'
--          left join DE1M.KISL_DWH_DIM_TERMINALS_HIST term
--                    on term.TERMINAL_ID = tran.TERMINAL
--                        and tran.TRANS_DATE between term.EFFECTIVE_FROM and term.EFFECTIVE_TO
--                        and term.DELETED_FLG = 'N'
-- where 1=0
-- -- or (client.PASSPORT_NUM = '5360 464827' and tran.TRANS_DATE between to_date('2021-03-01 23:16:34', 'YYYY-MM-DD HH24:MI:SS') and to_date('2021-03-02 01:52:34', 'YYYY-MM-DD HH24:MI:SS'))
-- -- or (client.PASSPORT_NUM = '8115 161624' and tran.TRANS_DATE between to_date('2021-03-01 01:54:34', 'YYYY-MM-DD HH24:MI:SS') and to_date('2021-03-01 04:18:45', 'YYYY-MM-DD HH24:MI:SS'))
-- or (client.PASSPORT_NUM = '3365 607538' and tran.TRANS_DATE between to_date('2021-03-03 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and to_date('2021-03-03 03:00:00', 'YYYY-MM-DD HH24:MI:SS'))
-- order by client.PASSPORT_NUM, tran.CARD_NUM, tran.TRANS_DATE;
--
-- 9386,5360 464827,4709 4592 6306 2366,Харабали,2021-03-02 00:16:34,A5830
-- 9386,5360 464827,4709 4592 6306 2366,Тюмень,2021-03-02 00:28:38,A9878
-- 9386,5360 464827,4709 4592 6306 2366,Тюмень,2021-03-02 00:43:40,P8667
-- 9386,5360 464827,4709 4592 6306 2366,Тюмень,2021-03-02 00:52:34,A9878
--
-- 3919,8115 161624,2979 3180 6757 7577,Истра,2021-03-01 02:54:34,P1550
-- 3919,8115 161624,2979 3180 6757 7577,Иркутск,2021-03-01 03:18:45,P1201
--
-- 7642,3365 607538,4311 1618 6334 8798,Нижний Новгород,2021-03-03 01:00:13,P1178
-- 7642,3365 607538,4311 1618 6334 8798,Москва,2021-03-03 01:03:29,P6335
-- 7642,3365 607538,4311 1618 6334 8798,Москва,2021-03-03 01:31:40,A3231
-- 7642,3365 607538,4311 1618 6334 8798,Москва,2021-03-03 01:59:40,P6335


-- 4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций
-- со следующим шаблоном – каждая последующая меньше предыдущей, при этом
-- отклонены все кроме последней. Последняя операция (успешная) в такой цепочке
-- считается мошеннической.
insert into DE1M.KISL_REP_FRAUD -- 2 rows affected in 1 s 155 ms
(EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select fraud_tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       4,
       current_date
-- select count(*) -- 10
-- select count(*) -- 2
from (
         select fraud_tran.*
              , lag(fraud_tran.OPER_TYPE, 1)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_OPER_TYPE
              , lag(fraud_tran.OPER_RESULT, 1)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_OPER_RESULT
              , lag(fraud_tran.TRANS_DATE, 1)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_TRANS_DATE
              , lag(fraud_tran.AMT, 1)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as THIRD_TRY_AMT

              , lag(fraud_tran.OPER_TYPE, 2)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_OPER_TYPE
              , lag(fraud_tran.OPER_RESULT, 2)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_OPER_RESULT
              , lag(fraud_tran.TRANS_DATE, 2)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_TRANS_DATE
              , lag(fraud_tran.AMT, 2)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as SECOND_TRY_AMT

              , lag(fraud_tran.OPER_TYPE, 3)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_OPER_TYPE
              , lag(fraud_tran.OPER_RESULT, 3)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_OPER_RESULT
              , lag(fraud_tran.TRANS_DATE, 3)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_TRANS_DATE
              , lag(fraud_tran.AMT, 3)
                    over (partition by fraud_tran.CARD_NUM order by fraud_tran.TRANS_DATE asc) as FIRST_TRY_AMT
         from DE1M.KISL_DWH_FACT_TRANSACTIONS fraud_tran
     ) fraud_tran
         left join DE1M.KISL_DWH_DIM_CARDS_HIST card
                   on fraud_tran.CARD_NUM = card.CARD_NUM
                       and fraud_tran.TRANS_DATE between card.EFFECTIVE_FROM and card.EFFECTIVE_TO
                       and card.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST acc
                   on acc.ACCOUNT_NUM = card.ACCOUNT_NUM
                       and fraud_tran.TRANS_DATE between acc.EFFECTIVE_FROM and acc.EFFECTIVE_TO
                       and acc.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST client
                   on acc.CLIENT = client.CLIENT_ID
                       and fraud_tran.TRANS_DATE between client.EFFECTIVE_FROM and client.EFFECTIVE_TO
                       and client.DELETED_FLG = 'N'
         left join DE1M.KISL_DWH_DIM_TERMINALS_HIST term
                   on term.TERMINAL_ID = fraud_tran.TERMINAL
                       and fraud_tran.TRANS_DATE between term.EFFECTIVE_FROM and term.EFFECTIVE_TO
                       and term.DELETED_FLG = 'N'
where fraud_tran.OPER_RESULT = 'SUCCESS'
  and fraud_tran.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
  and THIRD_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
  and THIRD_TRY_OPER_RESULT = 'REJECT'
  and THIRD_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
  and THIRD_TRY_TRANS_DATE < fraud_tran.TRANS_DATE
  and THIRD_TRY_AMT > fraud_tran.AMT
  and SECOND_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
  and SECOND_TRY_OPER_RESULT = 'REJECT'
  and SECOND_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
  and SECOND_TRY_TRANS_DATE < THIRD_TRY_TRANS_DATE
  and SECOND_TRY_AMT > THIRD_TRY_AMT
  and FIRST_TRY_OPER_TYPE in ('PAYMENT', 'WITHDRAW')
  and FIRST_TRY_OPER_RESULT = 'REJECT'
  and FIRST_TRY_TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
  and FIRST_TRY_TRANS_DATE < SECOND_TRY_TRANS_DATE
  and FIRST_TRY_AMT > SECOND_TRY_AMT
;
-- +-----------+-------------------+-------------------+---------+---------+-----------+--------+-------------------+---------------------+--------------------+-------------+--------------------+----------------------+---------------------+--------------+-------------------+---------------------+--------------------+-------------+
-- |TRANS_ID   |TRANS_DATE         |CARD_NUM           |OPER_TYPE|AMT      |OPER_RESULT|TERMINAL|THIRD_TRY_OPER_TYPE|THIRD_TRY_OPER_RESULT|THIRD_TRY_TRANS_DATE|THIRD_TRY_AMT|SECOND_TRY_OPER_TYPE|SECOND_TRY_OPER_RESULT|SECOND_TRY_TRANS_DATE|SECOND_TRY_AMT|FIRST_TRY_OPER_TYPE|FIRST_TRY_OPER_RESULT|FIRST_TRY_TRANS_DATE|FIRST_TRY_AMT|
-- +-----------+-------------------+-------------------+---------+---------+-----------+--------+-------------------+---------------------+--------------------+-------------+--------------------+----------------------+---------------------+--------------+-------------------+---------------------+--------------------+-------------+
-- |43853227164|2021-03-01 22:36:38|4202 3659 7174 8966|WITHDRAW |700.0000 |SUCCESS    |A8676   |WITHDRAW           |REJECT               |2021-03-01 22:36:01 |800          |WITHDRAW            |REJECT                |2021-03-01 22:34:58  |900           |WITHDRAW           |REJECT               |2021-03-01 22:33:59 |1000         |
-- |43861642040|2021-03-03 00:13:21|4709 4592 6306 2366|PAYMENT  |6824.3000|SUCCESS    |P9606   |PAYMENT            |REJECT               |2021-03-03 00:07:07 |7824.3       |PAYMENT             |REJECT                |2021-03-03 00:03:49  |8824.3        |PAYMENT            |REJECT               |2021-03-02 23:59:34 |9824.3       |
-- +-----------+-------------------+-------------------+---------+---------+-----------+--------+-------------------+---------------------+--------------------+-------------+--------------------+----------------------+---------------------+--------------+-------------------+---------------------+--------------------+-------------+


-- select fraud_tran.*
-- from DE1M.KISL_DWH_FACT_TRANSACTIONS fraud_tran
-- where fraud_tran.OPER_RESULT = 'SUCCESS'
--   and fraud_tran.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
-- --   and fraud_tran.TRANS_ID in ('43853227164', '43861642040')
--   and exists(select *
--              from DE1M.KISL_DWH_FACT_TRANSACTIONS third_trial
--                 , DE1M.KISL_DWH_FACT_TRANSACTIONS second_trial
--                 , DE1M.KISL_DWH_FACT_TRANSACTIONS first_trial
--              where third_trial.CARD_NUM = fraud_tran.CARD_NUM
--                and third_trial.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
--                and third_trial.OPER_RESULT = 'REJECT'
--                and third_trial.TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
--                and third_trial.TRANS_DATE < fraud_tran.TRANS_DATE
--                and third_trial.AMT > fraud_tran.AMT
--                and second_trial.CARD_NUM = fraud_tran.CARD_NUM
--                and second_trial.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
--                and second_trial.OPER_RESULT = 'REJECT'
--                and second_trial.TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
--                and second_trial.TRANS_DATE < third_trial.TRANS_DATE
--                and second_trial.AMT > third_trial.AMT
--                and first_trial.CARD_NUM = fraud_tran.CARD_NUM
--                and first_trial.OPER_TYPE in ('PAYMENT', 'WITHDRAW')
--                and first_trial.OPER_RESULT = 'REJECT'
--                and first_trial.TRANS_DATE between fraud_tran.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE
--                and first_trial.TRANS_DATE < second_trial.TRANS_DATE
--                and first_trial.AMT > second_trial.AMT
--                and not exists(select *
--                               from DE1M.KISL_DWH_FACT_TRANSACTIONS exc
--                               where exc.CARD_NUM = fraud_tran.CARD_NUM
--                                 and exc.TRANS_DATE > first_trial.TRANS_DATE
--                                 and exc.TRANS_DATE < fraud_tran.TRANS_DATE
--                                 and exc.TRANS_ID not in
--                                     (first_trial.TRANS_ID, second_trial.TRANS_ID, third_trial.TRANS_ID,
--                                      fraud_tran.TRANS_ID)
--                                 and (1 = 0
--                                   or exc.OPER_TYPE not in ('PAYMENT', 'WITHDRAW')
--                                   or exc.OPER_RESULT != 'REJECT'
--                                   or (exc.AMT < second_trial.AMT and
--                                       exc.TRANS_DATE between first_trial.TRANS_DATE - interval '20' minute and second_trial.TRANS_DATE)
--                                   or (exc.AMT < third_trial.AMT and
--                                       exc.TRANS_DATE between second_trial.TRANS_DATE - interval '20' minute and third_trial.TRANS_DATE)
--                                   or (exc.AMT < fraud_tran.AMT and
--                                       exc.TRANS_DATE between third_trial.TRANS_DATE - interval '20' minute and fraud_tran.TRANS_DATE)
--                                   )
--                  )
--     );

-- SUCCESS
-- REJECT
--
-- PAYMENT
-- DEPOSIT
-- WITHDRAW
--
