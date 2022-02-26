-- select last updated date
select to_char(
               coalesce(
                       (select cast(max(coalesce(stg.UPDATE_DT, stg.CREATE_DT)) as timestamp(7))
                        from DE1M.KISL_STG_CLIENT stg
                        where stg.CLIENT_ID in ( -- Exclude deleted rows
                            select act_rec.CLIENT_ID
                            from DE1M.KISL_STG_CLIENT_ACTIVE_REC act_rec
                        )
                       ),
                       (select max(meta.INTEGRATED_DT)
                        from DE1M.KISL_META_DWH meta
                        where meta.SCH_NAME = 'DE1M'
                          and meta.TBL_NAME = 'KISL_DWH_DIM_CLIENTS_HIST'),
                       to_timestamp('1800-01-01', 'YYYY-MM-DD')
                   ),
               'YYYY-MM-DD HH24:MI:SS.FF6') UPD_DT
from dual d