create table DE1M.KISL_DWH_FACT_PSSPRT_BLCKLST
    -- 2022-02-03 - v.kisel - ST в конце названия таблицы не влезло из-за ORA-00972: identifier is too long
(
    -- 2022-02-03 - v.kisel - Паспорт РФ состоит из 10 цифр, допускается 2 пробела.
    -- -- https://ru.wikipedia.org/wiki/%D0%9F%D0%B0%D1%81%D0%BF%D0%BE%D1%80%D1%82_%D0%B3%D1%80%D0%B0%D0%B6%D0%B4%D0%B0%D0%BD%D0%B8%D0%BD%D0%B0_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%BE%D0%B9_%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8#%D0%A1%D0%B5%D1%80%D0%B8%D1%8F_%D0%B8_%D0%BD%D0%BE%D0%BC%D0%B5%D1%80_%D0%BF%D0%B0%D1%81%D0%BF%D0%BE%D1%80%D1%82%D0%B0
    -- -- Но, возму такую же длину, как в таблице клиентов, т.к. полагаю, длину выбрали из соображений бизнеса.
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 т.к. NLS_CHARACTERSET = AL32UTF8, согласно рекомендациям:
    -- -- https://docs.oracle.com/database/121/NLSPG/ch2charset.htm#GUID-4E12D991-C286-4F1A-AFC6-F35040A5DE4F
    passport_num VARCHAR2(15) NULL,

    -- 2022-02-04 - v.kisel - Методом научного тыка определил, что в семпле входных данных используется тип данных Эксель "Дата".
    -- -- Date - самый короткий тип с фикс. длиной
    -- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#i54330
    entry_dt     DATE         NULL
);