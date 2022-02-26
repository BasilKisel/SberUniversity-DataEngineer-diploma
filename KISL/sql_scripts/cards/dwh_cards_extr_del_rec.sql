-- Add deleted rows into STG, so SCD2 rows will be closed
insert into DE1M.KISL_STG_CARD
    (CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT)
select trg.CARD_NUM, trg.ACCOUNT_NUM, current_date, current_date
from DE1M.KISL_DWH_DIM_CARDS_HIST trg
         left join DE1M.KISL_STG_CARD_ACTIVE_REC act_rec
                   on trg.CARD_NUM = trim(act_rec.CARD_NUM)
where act_rec.CARD_NUM is null
  and trg.DELETED_FLG = 'N'
  and trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
