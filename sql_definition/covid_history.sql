create table covid_history
(
        id              NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
        CONTINENT       VARCHAR2(256 CHAR),
        COUNTRY         VARCHAR2(256 CHAR),
        POPULATION      FLOAT,
        DATE_INFO       DATE,
        DATETIME_INFO   TIMESTAMP(6),
        NEW_CASES       FLOAT,
        ACTIVE_CASES    FLOAT,
        CRITICAL_CASES  FLOAT,
        RECOVERED_CASES FLOAT,
        TOTAL_CASES     FLOAT,
        NEW_DEATHS      FLOAT,
        TOTAL_DEATHS    FLOAT,
        TOTAL_TESTS     FLOAT
)
PARTITION BY LIST(CONTINENT)
(
        PARTITION P_ASIA VALUES('Asia'),
        PARTITION P_EUROPE VALUES('Europe'),
        PARTITION P_NORTHA VALUES('North-America'),
        PARTITION P_SOUTHA VALUES('South-America'),
        PARTITION P_AFRICA VALUES('Africa'),
        PARTITION P_OCEANIA VALUES('Oceania'),
        PARTITION P_UNKNOWN VALUES(DEFAULT)
)