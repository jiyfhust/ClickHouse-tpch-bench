package tpch

import (
	"context"
	"fmt"
)

var allTables []string

func init() {
	allTables = []string{"lineitem", "partsupp", "supplier", "part", "orders", "customer", "region", "nation"}
}

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableName string, action string) error {
	s := w.getState(ctx)
	fmt.Printf("%s %s\n", action, tableName)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	if w.cfg.CreateTiFlashReplica {
		fmt.Printf("creating tiflash replica for %s\n", tableName)
		replicaSQL := fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA 1", tableName)
		if _, err := s.Conn.ExecContext(ctx, replicaSQL); err != nil {
			return err
		}
	}
	return nil
}

// createTables creates tables schema.
func (w Workloader) createTables(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS nation (
    n_nationkey BIGINT NOT NULL,
    n_name CHAR(25) NOT NULL,
    n_regionkey BIGINT NOT NULL,
    n_comment VARCHAR(152),
    PRIMARY KEY (n_nationkey)
)
ENGINE = MergeTree ORDER BY n_nationkey`

	if err := w.createTableDDL(ctx, query, "nation", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS region (
    r_regionkey BIGINT NOT NULL,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152),
    PRIMARY KEY (r_regionkey)
)
ENGINE = MergeTree ORDER BY r_regionkey`
	if err := w.createTableDDL(ctx, query, "region", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS part (
	   p_partkey BIGINT NOT NULL,
	   p_name VARCHAR(55) NOT NULL,
	   p_mfgr CHAR(25) NOT NULL,
	   p_brand CHAR(10) NOT NULL,
	   p_type VARCHAR(25) NOT NULL,
	   p_size BIGINT NOT NULL,
	   p_container CHAR(10) NOT NULL,
	   p_retailprice DECIMAL(15, 2) NOT NULL,
	   p_comment VARCHAR(23) NOT NULL,
	   PRIMARY KEY (p_partkey)
	)
	ENGINE = MergeTree ORDER BY p_partkey`
	if err := w.createTableDDL(ctx, query, "part", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey BIGINT NOT NULL,
    s_name CHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey BIGINT NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal DECIMAL(15, 2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL,
    PRIMARY KEY (s_suppkey)
)
ENGINE = MergeTree ORDER BY s_suppkey`

	if err := w.createTableDDL(ctx, query, "supplier", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS partsupp (
	  ps_partkey BIGINT NOT NULL,
	  ps_suppkey BIGINT NOT NULL,
	  ps_availqty BIGINT NOT NULL,
	  ps_supplycost DECIMAL(15, 2) NOT NULL,
	  ps_comment VARCHAR(199) NOT NULL,
	  PRIMARY KEY (ps_partkey, ps_suppkey)
	)
	ENGINE = MergeTree ORDER BY (ps_partkey, ps_suppkey)`
	if err := w.createTableDDL(ctx, query, "partsupp", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS customer (
	  c_custkey BIGINT NOT NULL,
	  c_name VARCHAR(25) NOT NULL,
	  c_address VARCHAR(40) NOT NULL,
	  c_nationkey BIGINT NOT NULL,
	  c_phone CHAR(15) NOT NULL,
	  c_acctbal DECIMAL(15, 2) NOT NULL,
	  c_mktsegment CHAR(10) NOT NULL,
	  c_comment VARCHAR(117) NOT NULL,
	  PRIMARY KEY (c_custkey)
	)
	ENGINE = MergeTree ORDER BY c_custkey`
	if err := w.createTableDDL(ctx, query, "customer", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS orders (
	  o_orderkey BIGINT NOT NULL,
	  o_custkey BIGINT NOT NULL,
	  o_orderstatus CHAR(1) NOT NULL,
	  o_totalprice DECIMAL(15, 2) NOT NULL,
	  o_orderdate DATE NOT NULL,
	  o_orderpriority CHAR(15) NOT NULL,
	  o_clerk CHAR(15) NOT NULL,
	  o_shippriority BIGINT NOT NULL,
	  o_comment VARCHAR(79) NOT NULL,
	  PRIMARY KEY (o_orderkey)
	)
	ENGINE = MergeTree ORDER BY o_orderkey`
	if err := w.createTableDDL(ctx, query, "orders", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS lineitem (
	  l_orderkey BIGINT NOT NULL,
	  l_partkey BIGINT NOT NULL,
	  l_suppkey BIGINT NOT NULL,
	  l_linenumber BIGINT NOT NULL,
	  l_quantity DECIMAL(15, 2) NOT NULL,
	  l_extendedprice DECIMAL(15, 2) NOT NULL,
	  l_discount DECIMAL(15, 2) NOT NULL,
	  l_tax DECIMAL(15, 2) NOT NULL,
	  l_returnflag CHAR(1) NOT NULL,
	  l_linestatus CHAR(1) NOT NULL,
	  l_shipdate DATE NOT NULL,
	  l_commitdate DATE NOT NULL,
	  l_receiptdate DATE NOT NULL,
	  l_shipinstruct CHAR(25) NOT NULL,
	  l_shipmode CHAR(10) NOT NULL,
	  l_comment VARCHAR(44) NOT NULL,
	  PRIMARY KEY (l_orderkey, l_linenumber)
	)
	ENGINE = MergeTree ORDER BY (l_orderkey, l_linenumber)
	`
	if err := w.createTableDDL(ctx, query, "lineitem", "creating"); err != nil {
		return err
	}
	return nil
}

func (w *Workloader) dropTable(ctx context.Context) error {
	s := w.getState(ctx)

	for _, tbl := range allTables {
		fmt.Printf("DROP TABLE IF EXISTS %s\n", tbl)
		if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl)); err != nil {
			return err
		}
	}
	return nil
}
