insert into DE1M.KISL_DWH_FACT_PSSPRT_BLCKLST
    (PASSPORT_NUM, ENTRY_DT)
select PASSPORT_NUM, ENTRY_DT
from DE1M.KISL_STG_PASSPORT_BL