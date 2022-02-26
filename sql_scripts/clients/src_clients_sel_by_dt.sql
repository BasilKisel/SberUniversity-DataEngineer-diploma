select CLIENT_ID
     , LAST_NAME
     , FIRST_NAME
     , PATRONYMIC
     , to_char(DATE_OF_BIRTH, 'YYYY-MM-DD HH24:MI:SS')     as DATE_OF_BIRTH
     , PASSPORT_NUM
     , to_char(PASSPORT_VALID_TO, 'YYYY-MM-DD HH24:MI:SS') as PASSPORT_VALID_TO
     , PHONE
     , to_char(CREATE_DT, 'YYYY-MM-DD HH24:MI:SS')         as CREATE_DT
     , to_char(UPDATE_DT, 'YYYY-MM-DD HH24:MI:SS')         as UPDATE_DT
from BANK.CLIENTS
where CREATE_DT > cast(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS:FF7') as date)
   or UPDATE_DT > cast(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS:FF7') as date)
