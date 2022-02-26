select ACCOUNT
     , to_char(VALID_TO, 'YYYY-MM-DD HH24:MI:SS')  as VALID_TO
     , CLIENT
     , to_char(CREATE_DT, 'YYYY-MM-DD HH24:MI:SS') as CREATE_DT
     , to_char(UPDATE_DT, 'YYYY-MM-DD HH24:MI:SS') as UPDATE_DT
from BANK.ACCOUNTS
where CREATE_DT > cast(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS:FF7') as date)
   or UPDATE_DT > cast(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS:FF7') as date)
