create table DE1M.KISL_STG_CLIENT
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу-выгрузку из источника BANK.CLIENTS
    CLIENT_ID         VARCHAR2(20),
    LAST_NAME         VARCHAR2(100),
    FIRST_NAME        VARCHAR2(100),
    PATRONYMIC        VARCHAR2(100),
    DATE_OF_BIRTH     DATE,
    PASSPORT_NUM      VARCHAR2(15),
    PASSPORT_VALID_TO DATE,
    PHONE             VARCHAR2(20),
    CREATE_DT         DATE,
    UPDATE_DT         DATE,

    -- 2022-02-07 - v.kisel
    constraint CH_KISL_STG_CLIENT
        check (CREATE_DT is not null or UPDATE_DT is not null)
);