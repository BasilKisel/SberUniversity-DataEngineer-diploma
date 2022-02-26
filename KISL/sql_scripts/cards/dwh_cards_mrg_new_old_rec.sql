-- insert new rows, close updated rows
merge into DE1M.KISL_DWH_DIM_CARDS_HIST trg
using (select trim(src.CARD_NUM)                                             CARD_NUM
            , trim(src.ACCOUNT)                                              ACCOUNT
            , src.CREATE_DT
            , src.UPDATE_DT
            , case when (act_rec.CARD_NUM is not null) then 'N' else 'Y' end DELETED_FLG
       from DE1M.KISL_STG_CARD src
                left join de1m.KISL_STG_CARD_ACTIVE_REC act_rec on src.CARD_NUM = trim(act_rec.CARD_NUM)
) src
on (trg.CARD_NUM = src.CARD_NUM and trg.DELETED_FLG = 'N')
when matched then
    UPDATE -- close old updated records
    set EFFECTIVE_TO = src.UPDATE_DT
    where trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
      and trg.EFFECTIVE_FROM < src.UPDATE_DT
      and (src.DELETED_FLG = 'Y'
        or src.ACCOUNT <> trg.ACCOUNT_NUM or (src.ACCOUNT is null and trg.ACCOUNT_NUM is not null) or
           (src.ACCOUNT is not null and trg.ACCOUNT_NUM is null)
        )
when not matched then
    INSERT -- add new records
        (CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    values (src.CARD_NUM, src.ACCOUNT, coalesce(src.UPDATE_DT, src.CREATE_DT), to_date('9999-12-31', 'YYYY-MM-DD'), 'N')
