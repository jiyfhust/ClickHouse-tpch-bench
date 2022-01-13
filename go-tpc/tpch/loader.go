package tpch

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/go-tpc/pkg/load"
	"github.com/pingcap/go-tpc/tpch/dbgen"
)

type sqlLoader struct {
	*load.SQLBatchLoader
	context.Context
}

func (s *sqlLoader) InsertValue(value string) error {
	return s.SQLBatchLoader.InsertValue(s.Context, []string{value})
}

func (s *sqlLoader) Flush() error {
	return s.SQLBatchLoader.Flush(s.Context)
}

type orderLoader struct {
	sqlLoader
}

func (o *orderLoader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	v := fmt.Sprintf("(%d,%d,'%c','%s','%s','%s','%s',%d,'%s')",
		order.OKey,
		order.CustKey,
		order.Status,
		dbgen.FmtMoney(order.TotalPrice),
		order.Date,
		order.OrderPriority,
		order.Clerk,
		order.ShipPriority,
		order.Comment)
	return o.InsertValue(v)
}

type custLoader struct {
	sqlLoader
}

func (c *custLoader) Load(item interface{}) error {
	cust := item.(*dbgen.Cust)
	v := fmt.Sprintf("(%d,'%s','%s',%d,'%s','%s','%s','%s')",
		cust.CustKey,
		cust.Name,
		cust.Address,
		cust.NationCode,
		cust.Phone,
		dbgen.FmtMoney(cust.Acctbal),
		cust.MktSegment,
		cust.Comment)
	return c.InsertValue(v)
}

type lineItemloader struct {
	sqlLoader
}

func (l *lineItemloader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	for _, line := range order.Lines {
		v := fmt.Sprintf("(%d,%d,%d,%d,%d,'%s','%s','%s','%c','%c','%s','%s','%s','%s','%s','%s')",
			line.OKey,
			line.PartKey,
			line.SuppKey,
			line.LCnt,
			line.Quantity,
			dbgen.FmtMoney(line.EPrice),
			dbgen.FmtMoney(line.Discount),
			dbgen.FmtMoney(line.Tax),
			line.RFlag,
			line.LStatus,
			line.SDate,
			line.CDate,
			line.RDate,
			line.ShipInstruct,
			line.ShipMode,
			line.Comment,
		)
		if err := l.InsertValue(v); err != nil {
			return nil
		}
	}
	return nil
}

type nationLoader struct {
	sqlLoader
}

func (n *nationLoader) Load(item interface{}) error {
	nation := item.(*dbgen.Nation)
	v := fmt.Sprintf("(%d,'%s',%d,'%s')",
		nation.Code,
		nation.Text,
		nation.Join,
		nation.Comment)
	return n.InsertValue(v)
}

type partLoader struct {
	sqlLoader
}

func (p *partLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	v := fmt.Sprintf("(%d,'%s','%s','%s','%s',%d,'%s','%s','%s')",
		part.PartKey,
		part.Name,
		part.Mfgr,
		part.Brand,
		part.Type,
		part.Size,
		part.Container,
		dbgen.FmtMoney(part.RetailPrice),
		part.Comment)
	return p.InsertValue(v)
}

type partSuppLoader struct {
	sqlLoader
}

func (p *partSuppLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	for _, supp := range part.S {
		v := fmt.Sprintf("(%d,%d,%d,'%s','%s')",
			supp.PartKey,
			supp.SuppKey,
			supp.Qty,
			dbgen.FmtMoney(supp.SCost),
			supp.Comment)
		if err := p.InsertValue(v); err != nil {
			return err
		}
	}
	return nil
}

type suppLoader struct {
	sqlLoader
}

func (s *suppLoader) Load(item interface{}) error {
	supp := item.(*dbgen.Supp)
	v := fmt.Sprintf("(%d,'%s','%s',%d,'%s','%s','%s')",
		supp.SuppKey,
		supp.Name,
		supp.Address,
		supp.NationCode,
		supp.Phone,
		dbgen.FmtMoney(supp.Acctbal),
		supp.Comment)
	return s.InsertValue(v)
}

type regionLoader struct {
	sqlLoader
}

func (r *regionLoader) Load(item interface{}) error {
	region := item.(*dbgen.Region)
	v := fmt.Sprintf("(%d,'%s','%s')",
		region.Code,
		region.Text,
		region.Comment)
	return r.InsertValue(v)
}

func NewOrderLoader(ctx context.Context, db *sql.DB) *orderLoader {
	return &orderLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES `, 0, 0),
		ctx}}
}
func NewLineItemLoader(ctx context.Context, db *sql.DB) *lineItemloader {
	return &lineItemloader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES `, 0, 0),
		ctx}}
}
func NewCustLoader(ctx context.Context, db *sql.DB) *custLoader {
	return &custLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES `, 0, 0),
		ctx}}
}
func NewPartLoader(ctx context.Context, db *sql.DB) *partLoader {
	return &partLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) VALUES `, 0, 0),
		ctx}}
}
func NewPartSuppLoader(ctx context.Context, db *sql.DB) *partSuppLoader {
	return &partSuppLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) VALUES `, 0, 0),
		ctx}}
}
func NewSuppLoader(ctx context.Context, db *sql.DB) *suppLoader {
	return &suppLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES `, 0, 0),
		ctx}}
}
func NewNationLoader(ctx context.Context, db *sql.DB) *nationLoader {
	return &nationLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES `, 0, 0),
		ctx}}
}
func NewRegionLoader(ctx context.Context, db *sql.DB) *regionLoader {
	return &regionLoader{sqlLoader{load.NewSQLBatchLoader(db,
		`INSERT INTO region (r_regionkey, r_name, r_comment) VALUES `, 0, 0),
		ctx}}
}
