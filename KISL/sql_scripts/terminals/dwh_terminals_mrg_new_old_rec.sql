-- insert new rows, close updated rows
merge into DE1M.KISL_DWH_DIM_TERMINALS_HIST trg
using (select src.TERMINAL_ID
            , src.TERMINAL_TYPE
            , src.TERMINAL_CITY
            , src.TERMINAL_ADDRESS
            , (select COALESCE((CAST(max(meta.INTEGRATED_DT) AS DATE) + interval '1' day),
                               to_date('1900-01-01', 'YYYY-MM-DD'))
               from DE1M.KISL_META_DWH meta
               where meta.SCH_NAME = 'DE1M'
                 and meta.TBL_NAME = 'KISL_DWH_DIM_TERMINALS_HIST') as UPDATE_DT
            , case when (act_rec.TERMINAL_ID is not null) then 'N' else 'Y' end
                                                                    as DELETED_FLG
       from DE1M.KISL_STG_TERM src
                left join de1m.KISL_STG_TERM_ACTIVE_REC act_rec on src.TERMINAL_ID = act_rec.TERMINAL_ID
) src
on (trg.TERMINAL_ID = src.TERMINAL_ID and trg.DELETED_FLG = 'N')
when matched then
    UPDATE -- close old updated records
    set EFFECTIVE_TO = src.UPDATE_DT
    where trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
      and trg.EFFECTIVE_FROM < src.UPDATE_DT
      and (src.DELETED_FLG = 'Y'
        or src.TERMINAL_TYPE <> trg.TERMINAL_TYPE or (src.TERMINAL_TYPE is null and trg.TERMINAL_TYPE is not null) or
           (src.TERMINAL_TYPE is not null and trg.TERMINAL_TYPE is null)
        or src.TERMINAL_CITY <> trg.TERMINAL_CITY or (src.TERMINAL_CITY is null and trg.TERMINAL_CITY is not null) or
           (src.TERMINAL_CITY is not null and trg.TERMINAL_CITY is null)
        or src.TERMINAL_ADDRESS <> trg.TERMINAL_ADDRESS or
           (src.TERMINAL_ADDRESS is null and trg.TERMINAL_ADDRESS is not null) or
           (src.TERMINAL_ADDRESS is not null and trg.TERMINAL_ADDRESS is null)
        )
when not matched then
    INSERT -- add new records
    (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    values ( src.TERMINAL_ID
           , src.TERMINAL_TYPE
           , src.TERMINAL_CITY
           , src.TERMINAL_ADDRESS
           , src.UPDATE_DT
           , to_date('9999-12-31', 'YYYY-MM-DD')
           , 'N')
