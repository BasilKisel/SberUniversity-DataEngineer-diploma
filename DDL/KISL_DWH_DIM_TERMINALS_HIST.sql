create table DE1M.KISL_DWH_DIM_TERMINALS_HIST
(
    -- 2022-02-03 - v.kisel - Методом научного тыка обнаружил, что terminal_id состоит из буквы лат. алф. и 4-х цифр.
    -- -- Допущу, что в будущем может быть 2 буквы лат. алф. и 6 цифр.
    terminal_id VARCHAR2(8 CHAR) NOT NULL,

    -- 2022-02-03 - v.kisel - Методом научного тыка обнаружил, что terminal_type состоит из 3-х букв лат. алф.
    -- -- Сейчас имеет всего 2 значения: ATM и POS.
    -- -- Допущу, что 3-х букв лат. алф. хватит для будущих типов.
    terminal_type VARCHAR2(3 CHAR) NULL,

    -- 2022-02-03 - v.kisel - Методом научного тыка обнаружил, что terminal_city содержит кириллические буквы.
    -- -- Ограничусь длиной самого длинного названия нас. пункта https://ru.wikipedia.org/wiki/%D0%9B%D0%BB%D0%B0%D0%BD%D0%B2%D0%B0%D0%B9%D1%80-%D0%9F%D1%83%D0%BB%D0%BB%D0%B3%D1%83%D0%B8%D0%BD%D0%B3%D0%B8%D0%BB%D0%BB
    -- -- LENGTH('Llanfair­pwllgwyngyll­gogery­chwyrn­drobwll­llan­tysilio­gogo­goch')
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 т.к. NLS_CHARACTERSET = AL32UTF8, согласно рекомендациям:
    -- -- https://docs.oracle.com/database/121/NLSPG/ch2charset.htm#GUID-4E12D991-C286-4F1A-AFC6-F35040A5DE4F
    terminal_city VARCHAR2(66 CHAR) NULL,

    -- 2022-02-03 - v.kisel - Согласно ограничениями Excel, в поле адреса может поместиться 32767 символов.
    -- -- Возьму макс. возможную длину для NVARCHAR2.
    -- -- Смотри "Total number of characters that a cell can contain" в https://support.microsoft.com/en-us/office/excel-specifications-and-limits-1672b34d-7043-467e-8e27-269d656771c3
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 т.к. NLS_CHARACTERSET = AL32UTF8, согласно рекомендациям:
    -- -- https://docs.oracle.com/database/121/NLSPG/ch2charset.htm#GUID-4E12D991-C286-4F1A-AFC6-F35040A5DE4F
    terminal_address VARCHAR2(4000 CHAR) NULL,

    EFFECTIVE_FROM    DATE    NOT NULL,
    EFFECTIVE_TO      DATE    NOT NULL,
    DELETED_FLG       CHAR(1) NOT NULL,
    constraint C_KISL_DWH_DIM_TERM_HIST_DF
        check (DELETED_FLG in ('Y', 'N')),
    constraint C_KISL_DWH_DIM_TERM_HIST_ED
        check ( EFFECTIVE_FROM < EFFECTIVE_TO )
);