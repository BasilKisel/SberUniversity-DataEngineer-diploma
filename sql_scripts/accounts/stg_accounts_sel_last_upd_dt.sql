-- select last updated date
select to_char(
               coalesce(
                       (select cast(max(coalesce(stg.UPDATE_DT, stg.CREATE_DT)) as timestamp(7))
                        from DE1M.KISL_STG_ACC stg
                        where stg.ACCOUNT in ( -- Exclude deleted rows
                            select act_rec.ACCOUNT
                            from DE1M.KISL_STG_ACC_ACTIVE_REC act_rec
                        )
                       ),
                       (select max(meta.INTEGRATED_DT)
                        from DE1M.KISL_META_DWH meta
                        where meta.SCH_NAME = 'DE1M'
                          and meta.TBL_NAME = 'KISL_DWH_DIM_ACCOUNTS_HIST'),
                       to_timestamp('1800-01-01', 'YYYY-MM-DD')
                   ),
               'YYYY-MM-DD HH24:MI:SS.FF6') UPD_DT
from dual d