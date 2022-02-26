-- Add deleted rows into STG, so SCD2 rows will be closed
insert into DE1M.KISL_STG_CLIENT
( CLIENT_ID
, LAST_NAME
, FIRST_NAME
, PATRONYMIC
, DATE_OF_BIRTH
, PASSPORT_NUM
, PASSPORT_VALID_TO
, PHONE
, CREATE_DT
, UPDATE_DT)
select trg.CLIENT_ID
     , trg.LAST_NAME
     , trg.FIRST_NAME
     , trg.PATRONYMIC
     , trg.DATE_OF_BIRTH
     , trg.PASSPORT_NUM
     , trg.PASSPORT_VALID_TO
     , trg.PHONE
     , current_date
     , current_date
from DE1M.KISL_DWH_DIM_CLIENTS_HIST trg
         left join DE1M.KISL_STG_CLIENT_ACTIVE_REC act_rec
                   on trg.CLIENT_ID = act_rec.CLIENT_ID
where act_rec.CLIENT_ID is null
  and trg.DELETED_FLG = 'N'
  and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
