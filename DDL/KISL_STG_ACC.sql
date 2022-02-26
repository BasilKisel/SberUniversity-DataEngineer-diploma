create table DE1M.KISL_STG_ACC
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу-выгрузку из источника BANK.ACCOUNTS
    ACCOUNT   CHAR(20),
    VALID_TO  DATE,
    CLIENT    VARCHAR2(20),
    CREATE_DT DATE,
    UPDATE_DT DATE,

    -- 2022-02-07 - v.kisel
    constraint CH_KISL_STG_ACC_DT
        check (CREATE_DT is not null or UPDATE_DT is not null)
);