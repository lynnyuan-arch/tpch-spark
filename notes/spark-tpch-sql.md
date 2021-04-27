#1. Parquet
## 1.1 启动spark-sql
```bash
bin/spark-sql --conf spark.sql.warehouse.dir=/data/tmp/spark-warehouse
```

## 1.2 load数据到spark-warehouse
```sql
create database tpch;
use tpch;

CREATE TABLE IF NOT EXISTS csv_customer(
    C_CUSTKEY     INTEGER NOT NULL,
    C_NAME        STRING NOT NULL,
    C_ADDRESS     STRING NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       STRING NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  STRING NOT NULL,
    C_COMMENT     STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/customer.tbl";

CREATE TABLE IF NOT EXISTS csv_lineitem(
    L_ORDERKEY    INTEGER NOT NULL,
    L_PARTKEY     INTEGER NOT NULL,
    L_SUPPKEY     INTEGER NOT NULL,
    L_LINENUMBER  INTEGER NOT NULL,
    L_QUANTITY    DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
    L_DISCOUNT    DECIMAL(15,2) NOT NULL,
    L_TAX         DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG  CHAR(1) NOT NULL,
    L_LINESTATUS  CHAR(1) NOT NULL,
    L_SHIPDATE    DATE NOT NULL,
    L_COMMITDATE  DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT STRING NOT NULL,
    L_SHIPMODE     STRING NOT NULL,
    L_COMMENT      STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/lineitem.tbl";

CREATE TABLE IF NOT EXISTS csv_nation(
    N_NATIONKEY  INT  NOT NULL,
    N_NAME       STRING NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    STRING)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/nation.tbl";

CREATE TABLE IF NOT EXISTS csv_orders(
    O_ORDERKEY       INTEGER NOT NULL,
    O_CUSTKEY        INTEGER NOT NULL,
    O_ORDERSTATUS    STRING NOT NULL,
    O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  STRING NOT NULL,
    O_CLERK          STRING NOT NULL,
    O_SHIPPRIORITY   INTEGER NOT NULL,
    O_COMMENT        STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/orders.tbl";

CREATE TABLE IF NOT EXISTS csv_partsupp(
    PS_PARTKEY     INTEGER NOT NULL,
    PS_SUPPKEY     INTEGER NOT NULL,
    PS_AVAILQTY    INTEGER NOT NULL,
    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
    PS_COMMENT     STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/partsupp.tbl";

CREATE TABLE IF NOT EXISTS csv_part(
    P_PARTKEY     INTEGER NOT NULL,
    P_NAME        STRING NOT NULL,
    P_MFGR        STRING NOT NULL,
    P_BRAND       STRING NOT NULL,
    P_TYPE        STRING NOT NULL,
    P_SIZE        INTEGER NOT NULL,
    P_CONTAINER   STRING NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/part.tbl";

CREATE TABLE IF NOT EXISTS csv_region(
    R_REGIONKEY  INTEGER NOT NULL,
    R_NAME       STRING NOT NULL,
    R_COMMENT    STRING)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/region.tbl";

CREATE TABLE IF NOT EXISTS csv_supplier(
    S_SUPPKEY     INTEGER NOT NULL,
    S_NAME        STRING NOT NULL,
    S_ADDRESS     STRING NOT NULL,
    S_NATIONKEY   INTEGER NOT NULL,
    S_PHONE       STRING NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     STRING NOT NULL)
USING CSV
OPTIONS (
    header "false",
    delimiter "|"
) location "/opt/tpc-h-2.18.0_rc2/tables/supplier.tbl";


select * from csv_customer limit 10;
select * from csv_lineitem limit 10;
select * from csv_nation limit 10;
select * from csv_orders limit 10;
select * from csv_part limit 10;
select * from csv_partsupp limit 10;
select * from csv_region limit 10;
select * from csv_supplier limit 10;

-- Parquet Tables

CREATE TABLE IF NOT EXISTS customer(
    C_CUSTKEY     INTEGER NOT NULL,
    C_NAME        STRING NOT NULL,
    C_ADDRESS     STRING NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       STRING NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  STRING NOT NULL,
    C_COMMENT     STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/customer.parquet";

insert into customer select * from csv_customer;

CREATE TABLE IF NOT EXISTS lineitem(
    L_ORDERKEY    INTEGER NOT NULL,
    L_PARTKEY     INTEGER NOT NULL,
    L_SUPPKEY     INTEGER NOT NULL,
    L_LINENUMBER  INTEGER NOT NULL,
    L_QUANTITY    DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
    L_DISCOUNT    DECIMAL(15,2) NOT NULL,
    L_TAX         DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG  CHAR(1) NOT NULL,
    L_LINESTATUS  CHAR(1) NOT NULL,
    L_SHIPDATE    DATE NOT NULL,
    L_COMMITDATE  DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT STRING NOT NULL,
    L_SHIPMODE     STRING NOT NULL,
    L_COMMENT      STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/lineitem.parquet";

insert into lineitem select * from csv_lineitem;


CREATE TABLE IF NOT EXISTS nation(
    N_NATIONKEY  INT  NOT NULL,
    N_NAME       STRING NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    STRING)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/nation.parquet";

insert into nation select * from csv_nation;


CREATE TABLE IF NOT EXISTS orders(
    O_ORDERKEY       INTEGER NOT NULL,
    O_CUSTKEY        INTEGER NOT NULL,
    O_ORDERSTATUS    STRING NOT NULL,
    O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  STRING NOT NULL,
    O_CLERK          STRING NOT NULL,
    O_SHIPPRIORITY   INTEGER NOT NULL,
    O_COMMENT        STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/orders.parquet";

insert into orders select * from csv_orders;


CREATE TABLE IF NOT EXISTS partsupp(
    PS_PARTKEY     INTEGER NOT NULL,
    PS_SUPPKEY     INTEGER NOT NULL,
    PS_AVAILQTY    INTEGER NOT NULL,
    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
    PS_COMMENT     STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/partsupp.parquet";

insert into partsupp select * from csv_partsupp;

CREATE TABLE IF NOT EXISTS part(
    P_PARTKEY     INTEGER NOT NULL,
    P_NAME        STRING NOT NULL,
    P_MFGR        STRING NOT NULL,
    P_BRAND       STRING NOT NULL,
    P_TYPE        STRING NOT NULL,
    P_SIZE        INTEGER NOT NULL,
    P_CONTAINER   STRING NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/part.parquet";

insert into part select * from csv_part;

CREATE TABLE IF NOT EXISTS region(
    R_REGIONKEY  INTEGER NOT NULL,
    R_NAME       STRING NOT NULL,
    R_COMMENT    STRING)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/region.parquet";

insert into region select * from csv_region;


CREATE TABLE IF NOT EXISTS supplier(
    S_SUPPKEY     INTEGER NOT NULL,
    S_NAME        STRING NOT NULL,
    S_ADDRESS     STRING NOT NULL,
    S_NATIONKEY   INTEGER NOT NULL,
    S_PHONE       STRING NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     STRING NOT NULL)
USING PARQUET
location "/data/tmp/spark-warehouse/tpch/supplier.parquet";

insert into supplier select * from csv_supplier;
```