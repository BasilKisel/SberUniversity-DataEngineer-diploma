insert into DE1M.KISL_STG_TRAN
    (TRANS_ID, TRANS_DATE, AMT, CARD_NUM, OPER_TYPE, OPER_RESULT, TERMINAL)
values ( ?
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS')
       , replace(?, ',', '.')
       , ?
       , ?
       , ?
       , ?)