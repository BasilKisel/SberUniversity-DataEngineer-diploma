-- add active updated records
insert into DE1M.KISL_DWH_DIM_CLIENTS_HIST
(CLIENT_ID,
 LAST_NAME,
 FIRST_NAME,
 PATRONYMIC,
 DATE_OF_BIRTH,
 PASSPORT_NUM,
 PASSPORT_VALID_TO,
 PHONE,
 EFFECTIVE_FROM,
 EFFECTIVE_TO,
 DELETED_FLG)
select src.CLIENT_ID
     , src.LAST_NAME
     , src.FIRST_NAME
     , src.PATRONYMIC
     , src.DATE_OF_BIRTH
     , src.PASSPORT_NUM
     , src.PASSPORT_VALID_TO
     , src.PHONE
     , src.UPDATE_DT + interval '1' second
     , to_date('9999-12-31', 'YYYY-MM-DD')
     , case when (act_rec.CLIENT_ID is not null) then 'N' else 'Y' end
from DE1M.KISL_STG_CLIENT src
         left join DE1M.KISL_STG_CLIENT_ACTIVE_REC act_rec
                   on src.CLIENT_ID = act_rec.CLIENT_ID
         left join DE1M.KISL_DWH_DIM_CLIENTS_HIST trg
                   on src.CLIENT_ID = trg.CLIENT_ID
                       and trg.DELETED_FLG = 'N'
                       and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
where trg.CLIENT_ID is null
