-- Add deleted rows into STG, so SCD2 rows will be closed
insert into DE1M.KISL_STG_TERM
    (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS)
select trg.TERMINAL_ID, trg.TERMINAL_TYPE, trg.TERMINAL_CITY, trg.TERMINAL_ADDRESS
from DE1M.KISL_DWH_DIM_TERMINALS_HIST trg
         left join DE1M.KISL_STG_TERM_ACTIVE_REC act_rec
                   on trg.TERMINAL_ID = act_rec.TERMINAL_ID
where act_rec.TERMINAL_ID is null
  and trg.DELETED_FLG = 'N'
  and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
