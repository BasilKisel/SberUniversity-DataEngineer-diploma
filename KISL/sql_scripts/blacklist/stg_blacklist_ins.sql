insert into DE1M.KISL_STG_PASSPORT_BL
    (PASSPORT_NUM, ENTRY_DT)
values ( ?
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS'))