-- insert new rows, close updated rows
merge into DE1M.KISL_DWH_DIM_ACCOUNTS_HIST trg
using (select trim(src.ACCOUNT)                                             ACCOUNT
            , src.VALID_TO
            , src.CLIENT
            , src.CREATE_DT
            , src.UPDATE_DT
            , case when (act_rec.ACCOUNT is not null) then 'N' else 'Y' end DELETED_FLG
       from DE1M.KISL_STG_ACC src
                left join de1m.KISL_STG_ACC_ACTIVE_REC act_rec on trim(src.ACCOUNT) = trim(act_rec.ACCOUNT)
) src
on (trg.ACCOUNT_NUM = src.ACCOUNT and trg.DELETED_FLG = 'N')
when matched then
    UPDATE -- close old updated records
    set EFFECTIVE_TO = src.UPDATE_DT
    where trg.EFFECTIVE_TO = to_date('9999-12-31', 'YYYY-MM-DD')
      and trg.EFFECTIVE_FROM < src.UPDATE_DT
      and (src.DELETED_FLG = 'Y'
        or src.VALID_TO <> trg.VALID_TO or (src.VALID_TO is null and trg.VALID_TO is not null) or
           (src.VALID_TO is not null and trg.VALID_TO is null)
        or src.CLIENT <> trg.CLIENT or (src.CLIENT is null and trg.CLIENT is not null) or
           (src.CLIENT is not null and trg.CLIENT is null)
        )
when not matched then
    INSERT -- add new records
    (ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    values (src.ACCOUNT, src.VALID_TO, src.CLIENT, coalesce(src.UPDATE_DT, src.CREATE_DT),
            to_date('9999-12-31', 'YYYY-MM-DD'), 'N')
