-- insert new rows, close updated rows
merge into DE1M.KISL_DWH_DIM_CLIENTS_HIST trg
using (select src.CLIENT_ID
            , src.LAST_NAME
            , src.FIRST_NAME
            , src.PATRONYMIC
            , src.DATE_OF_BIRTH
            , src.PASSPORT_NUM
            , src.PASSPORT_VALID_TO
            , src.PHONE
            , src.CREATE_DT
            , src.UPDATE_DT
            , case when (act_rec.CLIENT_ID is not null) then 'N' else 'Y' end DELETED_FLG
       from DE1M.KISL_STG_CLIENT src
                left join de1m.KISL_STG_CLIENT_ACTIVE_REC act_rec on src.CLIENT_ID = act_rec.CLIENT_ID
) src
on (trg.CLIENT_ID = src.CLIENT_ID and trg.DELETED_FLG = 'N')
when matched then
    UPDATE -- close old updated records
    set EFFECTIVE_TO = src.UPDATE_DT
    where trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
      and trg.EFFECTIVE_FROM < src.UPDATE_DT
      and (src.DELETED_FLG = 'Y'
        or src.LAST_NAME <> trg.LAST_NAME or (src.LAST_NAME is null and trg.LAST_NAME is not null) or
           (src.LAST_NAME is not null and trg.LAST_NAME is null)
        or src.FIRST_NAME <> trg.FIRST_NAME or (src.FIRST_NAME is null and trg.FIRST_NAME is not null) or
           (src.FIRST_NAME is not null and trg.FIRST_NAME is null)
        or src.PATRONYMIC <> trg.PATRONYMIC or (src.PATRONYMIC is null and trg.PATRONYMIC is not null) or
           (src.PATRONYMIC is not null and trg.PATRONYMIC is null)
        or src.DATE_OF_BIRTH <> trg.DATE_OF_BIRTH or (src.DATE_OF_BIRTH is null and trg.DATE_OF_BIRTH is not null) or
           (src.DATE_OF_BIRTH is not null and trg.DATE_OF_BIRTH is null)
        or src.PASSPORT_NUM <> trg.PASSPORT_NUM or (src.PASSPORT_NUM is null and trg.PASSPORT_NUM is not null) or
           (src.PASSPORT_NUM is not null and trg.PASSPORT_NUM is null)
        or src.PASSPORT_VALID_TO <> trg.PASSPORT_VALID_TO or
           (src.PASSPORT_VALID_TO is null and trg.PASSPORT_VALID_TO is not null) or
           (src.PASSPORT_VALID_TO is not null and trg.PASSPORT_VALID_TO is null)
        or src.PHONE <> trg.PHONE or (src.PHONE is null and trg.PHONE is not null) or
           (src.PHONE is not null and trg.PHONE is null)
        )
when not matched then
    INSERT -- add new records
    ( CLIENT_ID
    , LAST_NAME
    , FIRST_NAME
    , PATRONYMIC
    , DATE_OF_BIRTH
    , PASSPORT_NUM
    , PASSPORT_VALID_TO
    , PHONE
    , EFFECTIVE_FROM
    , EFFECTIVE_TO
    , DELETED_FLG)
    values (src.CLIENT_ID,
            src.LAST_NAME,
            src.FIRST_NAME,
            src.PATRONYMIC,
            src.DATE_OF_BIRTH,
            src.PASSPORT_NUM
               , src.PASSPORT_VALID_TO
               , src.PHONE
               , coalesce(src.UPDATE_DT, src.CREATE_DT)
               , to_date('9999-12-31', 'YYYY-MM-DD')
               , 'N')
