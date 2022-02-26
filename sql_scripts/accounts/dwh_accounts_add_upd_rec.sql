-- add active updated records
insert into DE1M.KISL_DWH_DIM_ACCOUNTS_HIST
(ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select trim(src.ACCOUNT)
     , src.VALID_TO
     , src.CLIENT
     , src.UPDATE_DT + interval '1' second
     , to_date('9999-12-31', 'YYYY-MM-DD')
     , case when (act_rec.ACCOUNT is not null) then 'N' else 'Y' end
from DE1M.KISL_STG_ACC src
         left join DE1M.KISL_STG_ACC_ACTIVE_REC act_rec
                   on trim(src.ACCOUNT) = trim(act_rec.ACCOUNT)
         left join DE1M.KISL_DWH_DIM_ACCOUNTS_HIST trg
                   on trim(src.ACCOUNT) = trg.ACCOUNT_NUM
                       and trg.DELETED_FLG = 'N'
                       and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
where trg.ACCOUNT_NUM is null
