create table DE1M.KISL_DWH_FACT_TRANSACTIONS
(
    -- 2022-02-03 - v.kisel - Методом научного тыка увидел, что 11 значные числа больше чем 4.3e11.
    -- -- Я бы указал тип данных BIGINT, но могу сменить только на однородное поле согласно ER диаграмме из задания.
    -- -- Ожидаю, что будут только цифры и буквы лат. алф.
    -- -- Предположу, что если у банка всё пойдёт хорошо, число клиентов и число транзаций могут увеличиться на 2 порядка.
    -- -- Предположу, что текущий счётчик транзакций существует с 1990 года, когда выч. техника начала активно внедрятся в РФ.
    -- -- Предположу также, что модель и ETL будет работать около 150 лет (всё равно столько не проживу, не моя проблема).
    -- -- Итого: разрядность + разрядность_под_150_лет * разрадность_развития = (11) + (round_up((150 / (2020-1990) / 10)) * (x2 клиентов * x2 транзакций)
    -- -- -- = (11 + 2*2) + (1) = 16
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 согласно рекомендации Oracle для 11g
    -- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#sthref115
    trans_id    VARCHAR2(16 CHAR) NULL,

    -- 2022-02-04 - v.kisel - Методом научного тыка определил, что в семпле входных данных нет долей секунды.
    -- -- Date - самый короткий тип с фикс. длиной
    -- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#i54330
    trans_date  DATE              NULL,

    -- 2022-02-04 - v.kisel - Как BANK.CARDS.CARD_NUM
    -- 2022-02-04 - v.kisel - VARCHAR2 для уменьшения занимаемого места.
    card_num    VARCHAR2(20)          NULL,

    -- 2022-02-03 - v.kisel - Из данных видно, что используются только 3 значения: "DEPOSIT", "PAYMENT" и "WITHDRAW".
    -- -- Допущу, что в будущем будет использоваться только латиница и длина типа операции возрастёт не более чем в 2 раза.
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 согласно рекомендации Oracle для 11g
    -- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#sthref115
    oper_type   VARCHAR2(16 CHAR) NULL,

    -- 2022-02-04 - v.kisel - Допущу, что максимальный размер транзакций не будет превышать 10 млрд, а точность до 1 сотой копейки - достаточной.
    amt         DECIMAL(14, 4)    NULL,

    -- 2022-02-03 - v.kisel - Из данных видно, что используются только 2 значения: "REJECT" и "SUCCESS".
    -- -- Допущу, что в будущем будет использоваться только латиница и длина результата операции возрастёт не более чем в 2 раза.
    -- 2022-02-03 - v.kisel - Использую VARCHAR2 согласно рекомендации Oracle для 11g
    -- -- https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#sthref115
    oper_result VARCHAR2(14 CHAR) NULL,

    -- 2022-02-03 - v.kisel - Методом научного тыка обнаружил, что terminal состоит из буквы лат. алф. и 4-х цифр.
    -- -- Допущу, что в будущем может быть 2 буквы лат. алф. и что на 5 жителей РФ будет приходиться по 1-му терминалу: 150e6/5 = 30e6
    terminal    VARCHAR2(10 CHAR) NOT NULL
);