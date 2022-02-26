create table DE1M.KISL_DWH_DIM_CARDS_HIST
(
    -- 2022-02-03 - v.kisel - Структура повторяет таблицу из ER диаграммы.
    -- -- Типы взяты из источника BANK.CARDS, полагаю, их выбрали не случайно.
    -- -- Без тех. полей выгрузки.
    -- -- C тех. полями SCD2.

    -- 2022-02-04 - v.kisel - VARCHAR2 для уменьшения занимаемого места.
    -- 2022-02-04 - v.kisel - Ключевой аттрибут.
    CARD_NUM       VARCHAR2(20 CHAR) NOT NULL,

    -- 2022-02-04 - v.kisel - VARCHAR2 для уменьшения занимаемого места.
    ACCOUNT_NUM    VARCHAR2(20 CHAR),

    EFFECTIVE_FROM DATE    NOT NULL,
    EFFECTIVE_TO   DATE    NOT NULL,
    DELETED_FLG    CHAR(1) NOT NULL,
    constraint C_KISL_DWH_DIM_CARDS_HIST_DF
        check (DELETED_FLG in ('Y', 'N')),
    constraint C_KISL_DWH_DIM_CARDS_HIST_ET
        check ( EFFECTIVE_FROM < EFFECTIVE_TO )
);