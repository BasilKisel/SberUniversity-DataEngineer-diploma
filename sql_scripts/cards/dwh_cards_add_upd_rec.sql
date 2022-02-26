-- add active updated records
insert into DE1M.KISL_DWH_DIM_CARDS_HIST
    (CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select trim(src.CARD_NUM)
     , src.ACCOUNT
     , src.UPDATE_DT + interval '1' second
     , to_date('9999-12-31', 'YYYY-MM-DD')
     , case when (act_rec.CARD_NUM is not null) then 'N' else 'Y' end
from DE1M.KISL_STG_CARD src
         left join DE1M.KISL_STG_CARD_ACTIVE_REC act_rec
                   on trim(src.CARD_NUM) = trim(act_rec.CARD_NUM)
         left join DE1M.KISL_DWH_DIM_CARDS_HIST trg
                   on trim(src.CARD_NUM) = trg.CARD_NUM
                       and trg.DELETED_FLG = 'N'
                       and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
where trg.CARD_NUM is null
