create table DE1M.KISL_META_DWH
(
    -- 2022-02-06 - v.kisel - max identifier's len is 30 char
    -- -- https://docs.oracle.com/en/database/oracle/oracle-data-access-components/19.3/odpnt/EFCoreIdentifier.html
    sch_name      varchar2(30 char) not null,
    tbl_name      varchar2(30 char) not null,

    -- 2022-02-06 - v.kisel - Выбрал макс. точность на случай добавления таблицы с макс. точностью датывремени в будущем.
    -- 2022-02-09 - v.kisel - Ограничил точность до 6 знаков долей секунды, т.к. datetime в Python не может больше.
    integrated_dt timestamp(6)      not null,

    constraint PK__KISL_META_DWH
        primary key (sch_name, tbl_name)
);