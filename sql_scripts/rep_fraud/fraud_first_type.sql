-- 1. Совершение операции при просроченном или заблокированном паспорте.
insert into DE1M.KISL_REP_FRAUD
    (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
select tran.TRANS_DATE,
       client.PASSPORT_NUM,
       client.LAST_NAME || ' ' || client.FIRST_NAME || ' ' || client.PATRONYMIC,
       client.PHONE,
       1,
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
         left join DE1M.KISL_DWH_FACT_PSSPRT_BLCKLST ps_bl
                   on ps_bl.PASSPORT_NUM = client.PASSPORT_NUM
                       and ps_bl.ENTRY_DT < tran.TRANS_DATE
where (ps_bl.PASSPORT_NUM is not null
    or (client.PASSPORT_VALID_TO + interval '1' day < tran.TRANS_DATE))