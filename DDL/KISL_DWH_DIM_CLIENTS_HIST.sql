create table DE1M.KISL_DWH_DIM_CLIENTS_HIST
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу из ER диаграммы.
    -- -- Типы взяты из источника BANK.CLIENTS, полагаю, их выбрали не случайно.
    -- -- Без тех. полей выгрузки.
    -- -- C тех. полями SCD2.

    CLIENT_ID         VARCHAR2(20),
    LAST_NAME         VARCHAR2(100),
    FIRST_NAME        VARCHAR2(100),
    PATRONYMIC        VARCHAR2(100),
    DATE_OF_BIRTH     DATE,
    PASSPORT_NUM      VARCHAR2(15),
    PASSPORT_VALID_TO DATE,
    PHONE             VARCHAR2(20),

    EFFECTIVE_FROM    DATE    NOT NULL,
    EFFECTIVE_TO      DATE    NOT NULL,
    DELETED_FLG       CHAR(1) NOT NULL,
    constraint C_KISL_DWH_DIM_CL_HIST_DF
        check (DELETED_FLG in ('Y', 'N')),
    constraint C_KISL_DWH_DIM_CL_HIST_ED
        check ( EFFECTIVE_FROM < EFFECTIVE_TO )
);