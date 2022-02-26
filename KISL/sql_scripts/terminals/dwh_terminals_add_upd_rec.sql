-- add active updated records
insert into DE1M.KISL_DWH_DIM_TERMINALS_HIST
(TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select src.TERMINAL_ID
     , src.TERMINAL_TYPE
     , src.TERMINAL_CITY
     , src.TERMINAL_ADDRESS
     , (select COALESCE((CAST(max(meta.INTEGRATED_DT) AS DATE) + interval '1' day),
                        to_date('1900-01-01', 'YYYY-MM-DD'))
        from DE1M.KISL_META_DWH meta
        where meta.SCH_NAME = 'DE1M'
          and meta.TBL_NAME = 'KISL_DWH_DIM_TERMINALS_HIST') + interval '1' second
     , to_date('9999-12-31', 'YYYY-MM-DD')
     , case when (act_rec.TERMINAL_ID is not null) then 'N' else 'Y' end
from DE1M.KISL_STG_TERM src
         left join DE1M.KISL_STG_TERM_ACTIVE_REC act_rec
                   on src.TERMINAL_ID = act_rec.TERMINAL_ID
         left join DE1M.KISL_DWH_DIM_TERMINALS_HIST trg
                   on src.TERMINAL_ID = trg.TERMINAL_ID
                       and trg.DELETED_FLG = 'N'
                       and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
where trg.TERMINAL_ID is null
