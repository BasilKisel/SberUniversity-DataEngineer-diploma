select max(1) as has_new_data
from DE1M.KISL_META_REP rep_m
where rep_m.sch_name = 'DE1M'
  and rep_m.tbl_name = 'KISL_REP_FRAUD'
  and (coalesce((rep_m.acc_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_DIM_ACCOUNTS_HIST')
    or coalesce((rep_m.card_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_DIM_CARDS_HIST')
    or coalesce((rep_m.client_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_DIM_CLIENTS_HIST')
    or coalesce((rep_m.passport_bl_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_FACT_PSSPRT_BLCKLST')
    or coalesce((rep_m.term_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_DIM_TERMINALS_HIST')
    or coalesce((rep_m.tran_integrated_dt), to_date('1800-01-01', 'YYYY-MM-DD'))
           < (select coalesce(max(dwh_m.INTEGRATED_DT), to_date('1800-01-01', 'YYYY-MM-DD'))
              from DE1M.KISL_META_DWH dwh_m
              where dwh_m.SCH_NAME = 'DE1M'
                and dwh_m.TBL_NAME = 'KISL_DWH_FACT_TRANSACTIONS')
    )