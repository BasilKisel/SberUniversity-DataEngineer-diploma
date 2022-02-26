create table DE1M.KISL_STG_CARD
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу-выгрузку из источника BANK.CARDS
    CARD_NUM  CHAR(20),
    ACCOUNT   CHAR(20),
    CREATE_DT DATE,
    UPDATE_DT DATE,

    -- 2022-02-07 - v.kisel
    constraint CH_KISL_STG_CARD
        check (CREATE_DT is not null or UPDATE_DT is not null)
);