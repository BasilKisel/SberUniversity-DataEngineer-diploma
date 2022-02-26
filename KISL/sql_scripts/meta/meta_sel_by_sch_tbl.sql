select coalesce(to_char(max(INTEGRATED_DT), 'YYYY-MM-DD HH24:MI:SS.FF6'), '1800-01-01 00:00:00.000000') as INTEGRATED_DT
from de1m.KISL_META_DWH
where SCH_NAME = ?
  and TBL_NAME = ?
