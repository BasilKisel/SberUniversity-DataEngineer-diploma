create table DE1M.KISL_REP_FRAUD
(
    -- 2022-02-04 - v.kisel - События должны приходить из транзакций, так что тип времени беру как TRANS_DATE.
    event_dt   DATE          NOT NULL,

    -- 2022-02-04 - v.kisel - Беру тип как для PASSPORT_NUM из таблицы клиентов.
    passport   VARCHAR2(15)  NULL,

    -- 2022-02-04 - v.kisel - Беру тип как для FIRST_NAME, PATRONYMIC, LAST_NAME из таблицы клиентов с конкатенаций через пробел.
    fio        VARCHAR2(302) NULL,

    -- 2022-02-04 - v.kisel - Беру тип как для PHONE из таблицы клиентов.
    phone      VARCHAR2(20)  NULL,

    -- 2022-02-10 - v.kisel - ID операции мошеничества, от 1 до 4.
    event_type SMALLINT      NOT NULL,

-- 2022-02-04 - v.kisel - Тех поле; указано, что может быть записано время.
-- -- Полагаю, что доли секунды не нужны.
-- -- Date - самый короткий тип с фикс. длиной
-- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#i54330
    report_dt  DATE          NOT NULL
);