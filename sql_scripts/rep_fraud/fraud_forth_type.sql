-- 4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций
-- со следующим шаблоном – каждая последующая меньше предыдущей, при этом
-- отклонены все кроме последней. Последняя операция (успешная) в такой цепочке
-- считается мошеннической.
insert into DE1M.KISL_REP_FRAUD
    (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select fraud_tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       4,
       to_date(?, 'YYYY-MM-DD HH24:MI:SS')
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
