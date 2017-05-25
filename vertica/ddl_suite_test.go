package vertica

import (
	"github.com/b13f/repligator/isql"
	"github.com/stretchr/testify/suite"
)

type DDLTestSuite struct {
	suite.Suite
	v *Cache
}

func (s *DDLTestSuite) SetupSuite() {
	c := Config{
		Odbc:     `Vertica`,
		Host:     `127.0.0.1`,
		Port:     `5433`,
		User:     `dbadmin`,
		Password: ``,
		Database: `docker`,
		DataDir:  `/tmp`,
	}

	var err error
	if s.v, err = Init(c); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *DDLTestSuite) TestCreateSchema() {
	t := s.v.getSchemaSQL(isql.CreateSchema{Name: `test`})

	s.Equal([]string{`CREATE SCHEMA IF NOT EXISTS "test"`}, t)
}

func (s *DDLTestSuite) TestCreateTable() {
	t := s.v.GetTableSQL(isql.CreateTable{
		Table: isql.Table{Schema: `testing`, Name: `test`},
		Columns: []isql.Column{
			{Name: `id`, Type: `bigint(20)`},
			{Name: `value`, Type: `varchar(255)`},
			{Name: `date`, Type: `timestamp`},
		},
		Constraints: []isql.Constraint{
			{Type: isql.Primary, Columns: []string{`id`}},
			{Type: isql.Unique, Columns: []string{`value`, `date`}},
		},
	})

	s.Equal([]string{`CREATE TABLE IF NOT EXISTS "testing"."test"
(
"id" NUMBER,
"value" VARCHAR(255),
"date" TIMESTAMPTZ,
PRIMARY KEY ("id") ENABLED,
UNIQUE ("value","date") ENABLED) ORDER BY "id"`}, t)

	t = s.v.GetTableSQL(isql.CreateTable{
		Table: isql.Table{Schema: `testing`, Name: `test`},
		Columns: []isql.Column{
			{Name: `id`, Type: `bigint(20)`},
			{Name: `value`, Type: `enum("first","second","last")`},
			{Name: `date`, Type: `timestamp`},
		},
		Constraints: []isql.Constraint{
			{Type: isql.Unique, Columns: []string{`id`, `date`}},
		},
	})

	s.Equal([]string{`CREATE TABLE IF NOT EXISTS "testing"."test"
(
"id" NUMBER,
"value" VARCHAR(29) /* ENUM: enum("first","second","last")*/,
"date" TIMESTAMPTZ,
UNIQUE ("id","date") ENABLED) ORDER BY "id","date" SEGMENTED BY hash("id","date") ALL NODES`,
		`COMMENT ON TABLE "testing"."test" IS 'enum(2["first","second","last"])'`}, t)

}

func (s *DDLTestSuite) TestCreateTableLike() {
	t := s.v.getTableLikeSQL(isql.CreateTableLike{
		Table:     isql.Table{Schema: `testing`, Name: `test1`},
		LikeTable: isql.Table{Schema: `testing2`, Name: `test2`},
	})

	s.Equal([]string{`CREATE TABLE IF NOT EXISTS "testing"."test1" LIKE "testing2"."test2"`}, t)
}

func (s *DDLTestSuite) TestTruncate() {
	t := s.v.getTruncateSQL(isql.TruncateTable{isql.Table{Schema: `testing`, Name: `test1`}})

	s.Equal([]string{`TRUNCATE TABLE "testing"."test1"`}, t)
}

func (s *DDLTestSuite) TestRenameTable() {
	t := s.v.getRenameSQL([]isql.RenameTable{{
		From: isql.Table{Schema: `testing`, Name: `test1`},
		To:   isql.Table{Schema: `testing1`, Name: `test1`},
	}})

	s.Equal([]string{
		`CREATE TABLE IF NOT EXISTS "testing1"."test1" AS SELECT * FROM "testing"."test1"`,
		`DROP TABLE IF EXISTS "testing"."test1" CASCADE`,
	}, t)
}

func (s *DDLTestSuite) TestDropTable() {
	t := s.v.GetDropSQL([]isql.DropTable{
		{isql.Table{Schema: `testing`, Name: `test1`}},
		{isql.Table{Schema: `testing1`, Name: `test1`}},
	})

	s.Equal([]string{
		`DROP TABLE IF EXISTS "testing"."test1" CASCADE`,
		`DROP TABLE IF EXISTS "testing1"."test1" CASCADE`,
	}, t)
}

func (s *DDLTestSuite) TestAlterTable() {
	//for alter need existed table in vertica
	_, _ = s.v.Exec([]string{
		`CREATE SCHEMA IF NOT EXISTS altertest`,
		`CREATE TABLE IF NOT EXISTS altertest.test (id NUMBER,val VARCHAR(60),val2 VARCHAR(60))`,
		`COMMENT ON TABLE altertest.test IS 'enum(2["first","second","last"]);enum(3["f","s","l"])'`,
	})

	t, err := s.v.getAlterSQL(isql.AlterTable{
		Table: isql.Table{Name: `test`, Schema: `altertest`},
		AddColumns: []isql.Column{
			{Name: `value`, Type: `varchar(255)`},
			{Name: `date`, Type: `timestamp`},
		},
	})

	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal([]string{
		`ALTER TABLE "altertest"."test" ADD COLUMN "value" VARCHAR(255)`,
		`ALTER TABLE "altertest"."test" ADD COLUMN "date" TIMESTAMPTZ`,
		`COMMENT ON TABLE "altertest"."test" IS 'enum(2["first","second","last"]);enum(3["f","s","l"])'`,
	}, t)

	t, err = s.v.getAlterSQL(isql.AlterTable{
		Table: isql.Table{Name: `test`, Schema: `altertest`},
		DropColumns: []isql.Column{
			{Name: `val`},
		},
	})

	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal([]string{
		`ALTER TABLE "altertest"."test" DROP COLUMN "val" CASCADE`,
		`COMMENT ON TABLE "altertest"."test" IS 'enum(2["f","s","l"])'`,
	}, t)

	t, err = s.v.getAlterSQL(isql.AlterTable{
		Table: isql.Table{Name: `test`, Schema: `altertest`},
		DropColumns: []isql.Column{
			{Name: `val`},
		},
		AddColumns: []isql.Column{
			{Name: `enum`, Type: `enum('1','2','3')`},
		},
		AddConstraints: []isql.Constraint{
			{Type: isql.Primary, Columns: []string{`enum`}},
			{Type: isql.Unique, Columns: []string{`val2`, `id`}},
		},
	})

	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal([]string{
		`ALTER TABLE "altertest"."test" DROP COLUMN "val" CASCADE`,
		`ALTER TABLE "altertest"."test" ADD COLUMN "enum" VARCHAR(17) /* ENUM: enum('1','2','3')*/`,
		`ALTER TABLE "altertest"."test" ADD PRIMARY KEY ("enum") ENABLED`,
		`ALTER TABLE "altertest"."test" ADD UNIQUE ("val2","id") ENABLED`,
		`COMMENT ON TABLE "altertest"."test" IS 'enum(3[''1'',''2'',''3'']);enum(2["f","s","l"])'`,
	}, t)
}
