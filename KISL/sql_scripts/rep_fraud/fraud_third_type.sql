-- 3. Совершение операций в разных городах в течение одного часа.
insert into DE1M.KISL_REP_FRAUD
    (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       3,
       to_date(?, 'YYYY-MM-DD HH24:MI:SS')
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
          )