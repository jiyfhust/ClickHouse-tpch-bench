# ClickHouse-tpch-bench
A tpc-h bench for ClickHouse.

Code modified from https://github.com/pingcap/tiup.

## 编译

```
git clone git@github.com:jiyfhust/ClickHouse-tpch-bench.git
cd ClickHouse-tpch-bench
make
```

## 用法

```
./tiup-bench tpch -D bench -H ck_host -P 9004 -U jiyf -p abc --sf=1 prepare

./tiup-bench tpch -D bench -H ck_host -P 9004 -U jiyf -p abc --sf=1 run

./tiup-bench tpch -D bench -H ck_host -P 9004 -U jiyf -p abc --sf=1 cleanup
```

9004 is mysql port of clickhouse

## 说明

在 tiup tpc-h 上进行了一些修改：

1. 修改了建表 SQL 以兼容 ClickHouse
2. 修改了部分语句参数，例如 sql 中 date_add 函数 ClickHouse 不兼，将 date_add('1995-01-01', interval '3' month) 改为 ’1995-04-01‘
3. 修改了不兼容的类型计算，例如数值 0.06 - 0.01 改为 toDecimal64(0.06 - 0.01, 2) 以兼容类型比较运算
4. 对部分 sql 语句进行了更改，ClickHouse 对于 tpc-h 上很多 sql 语法上不支持，更改过程中**有的改变了语义**，后面会专门列出。
5. ClickHouse 对于复杂查询性能较差，**q19 没有跑出来结果，所以执行完 q18 就可以退出了，q20、q21、q22 没有对语句进行兼容性改写**。

### q2 修改后语义有改变

```
select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 30 and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier,
nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100;

改写后语义有改变

/*PLACEHOLDER*/ select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 30 and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and ps_supplycost = ( select min(ps_supplycost) from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' ) order by s_acctbal desc, n_name, s_name, p_partkey limt 100;
```

## q4 修改后语义未变

```
/*PLACEHOLDER*/ select o_orderpriority, count(*) as order_count from orders where o_orderdate >= '1995-01-01' and o_orderdate < '1995-04-01' and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority;

改为后，语义没有改变。

select o_orderpriority, count(*) as order_count from orders where o_orderdate >= '1995-01-01' and o_orderdate < '1995-04-01' and o_orderkey in ( select o_orderkey from lineitem,orders where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority;
```

## q17 修改后语义有改变

```
/*PLACEHOLDER*/ select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand =
'Brand#44' and p_container = 'WRAP PKG' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey );

改为了，语义变了 

select sum(l_extendedprice) / toDecimal64(7.0, 2) as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand =
'Brand#44' and p_container = 'WRAP PKG' and toFloat64(l_quantity) < ( select 0.2 * avg(l_quantity) from lineitem,part where l_partkey
= p_partkey and p_brand = 'Brand#44' and p_container = 'WRAP PKG' );
```

