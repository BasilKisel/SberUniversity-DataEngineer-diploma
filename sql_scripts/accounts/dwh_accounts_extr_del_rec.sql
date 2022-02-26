-- Add deleted rows into STG, so SCD2 rows will be closed
insert into DE1M.KISL_STG_ACC
    (ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT)
select trg.ACCOUNT_NUM, trg.VALID_TO, trg.CLIENT, current_date, current_date
from DE1M.KISL_DWH_DIM_ACCOUNTS_HIST trg
         left join DE1M.KISL_STG_ACC_ACTIVE_REC act_rec
                   on trg.ACCOUNT_NUM = trim(act_rec.ACCOUNT)
where act_rec.ACCOUNT is null
  and trg.DELETED_FLG = 'N'
  and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
