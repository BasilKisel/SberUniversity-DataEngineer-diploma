-- update metadata
merge into DE1M.KISL_META_DWH meta
using (select ? SCH_NAME, ? TBL_NAME, to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS.FF6') UPD_DT
    from dual
) stg_stats
ON (meta.SCH_NAME = stg_stats.SCH_NAME and meta.TBL_NAME = stg_stats.TBL_NAME)
when matched then
    update
    set INTEGRATED_DT = coalesce(stg_stats.UPD_DT, meta.INTEGRATED_DT)
when not matched then
    insert
        (SCH_NAME, TBL_NAME, INTEGRATED_DT)
    VALUES (stg_stats.SCH_NAME, stg_stats.TBL_NAME,
            coalesce(stg_stats.UPD_DT, to_timestamp('1800-01-01', 'YYYY-MM-DD')))
