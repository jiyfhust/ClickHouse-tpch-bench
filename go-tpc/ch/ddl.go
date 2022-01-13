package ch

import (
	"context"
	"fmt"
)

var allTables []string

func init() {
	allTables = []string{"customer", "district", "history", "item", "new_order", "order_line", "orders", "region", "warehouse",
		"nation", "stock", "supplier"}
}

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableName string, action string) error {
	s := w.getState(ctx)
	fmt.Printf("%s %s\n", action, tableName)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
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
)`

	if err := w.createTableDDL(ctx, query, "nation", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS region (
    r_regionkey BIGINT NOT NULL,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152),
    PRIMARY KEY (r_regionkey)
)`
	if err := w.createTableDDL(ctx, query, "region", "creating"); err != nil {
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
)`
	if err := w.createTableDDL(ctx, query, "supplier", "creating"); err != nil {
		return err
	}

	return nil
}
