insert into DE1M.KISL_STG_CARD (CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT)
values (?,
        ?,
        to_date(?, 'YYYY-MM-DD HH24:MI:SS'),
        to_date(?, 'YYYY-MM-DD HH24:MI:SS'))
