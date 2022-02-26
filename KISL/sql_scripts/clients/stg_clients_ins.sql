insert into DE1M.KISL_STG_CLIENT ( CLIENT_ID
                                 , LAST_NAME
                                 , FIRST_NAME
                                 , PATRONYMIC
                                 , DATE_OF_BIRTH
                                 , PASSPORT_NUM
                                 , PASSPORT_VALID_TO
                                 , PHONE
                                 , CREATE_DT
                                 , UPDATE_DT)
values ( ?
       , ?
       , ?
       , ?
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS')
       , ?
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS')
       , ?
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS')
       , to_date(?, 'YYYY-MM-DD HH24:MI:SS'))
