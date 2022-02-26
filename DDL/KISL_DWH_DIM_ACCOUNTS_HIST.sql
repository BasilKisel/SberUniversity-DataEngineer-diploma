create table DE1M.KISL_DWH_DIM_ACCOUNTS_HIST
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу из ER диаграммы.
    -- -- Типы взяты из источника BANK.ACCOUNTS, полагаю, их выбрали не случайно.
    -- -- Без тех. полей выгрузки.
    -- -- C тех. полями SCD2.

    -- 2022-02-04 - v.kisel - VARCHAR2 для уменьшения занимаемого места.
    -- 2022-02-04 - v.kisel - Ключевой аттрибут.
    ACCOUNT_NUM    VARCHAR2(20 BYTE) NOT NULL,

    VALID_TO       DATE,
    CLIENT         VARCHAR2(20),

    EFFECTIVE_FROM DATE              NOT NULL,
    EFFECTIVE_TO   DATE              NOT NULL,
    DELETED_FLG    CHAR(1)           NOT NULL,
    constraint C_KISL_DWH_DIM_AC_HIST_DF
        check (DELETED_FLG in ('Y', 'N')),
    constraint C_KISL_DWH_DIM_AC_HIST_ED
        check ( EFFECTIVE_FROM < EFFECTIVE_TO )
);