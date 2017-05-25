package ddlparser

import (
	"errors"
	"regexp"
	"strings"
	"text/scanner"

	"github.com/b13f/repligator/isql"
)

// ddl cases
const (
	createTable = iota
	createDatabase
	createSchema
	truncate
	renameTable
	dropTable
	alterTable
)

var have = []string{
	"CREATE TABLE",
	"CREATE DATABASE",
	"CREATE SCHEMA",
	"TRUNCATE",
	"RENAME TABLE",
	"DROP TABLE",
	"ALTER TABLE"}

//Ddlcase return internal struct for MySQL ddl expression
func Ddlcase(sql string, schema string) interface{} {
	dp, err := newDdlParser(sql, schema)

	if err != nil {
		return err
	}

	if dp.skip {
		return nil
	}

	return dp.getTypeStruct()
}

var comment = regexp.MustCompile("/\\*.*?\\*/")

// del comment in sql
func sqlfilter(sql string) (filtered string) {
	//comments
	filtered = comment.ReplaceAllLiteralString(sql, "")

	return
}

var ignoreOne = []string{"GRANT", "REVOKE", "FLUSH", "ANALYZE"}
var ignoreTwo = []string{
	"ALTER USER",
	"CREATE USER",
	"DROP USER",
	"RENAME USER",
	"SET PASSWORD",
	"ALTER EVENT",
	"ALTER FUNCTION",
	"ALTER INSTANCE",
	"ALTER PROCEDURE",
	"ALTER SERVER",
	"ALTER TABLESPACE",
	"ALTER VIEW",
	"CREATE EVENT",
	"CREATE FUNCTION",
	"CREATE INDEX",
	"CREATE PROCEDURE",
	"CREATE SERVER",
	"CREATE TRIGGER",
	"CREATE VIEW",
	"CREATE TEMPORARY",
	"DROP EVENT",
	"DROP FUNCTION",
	"DROP PROCEDURE",
	"DROP FUNCTION",
	"DROP TRIGGER",
	"DROP VIEW",
	"DROP TEMPORARY",
	"DROP INDEX",
}

// helper strings.ToUpper
func up(str string) string {
	return strings.ToUpper(str)
}

// helper strings.Trim for [`] ['] ["]
func tr(str string) string {
	return strings.Trim(str, "`'\"")
}

type ddlParser struct {
	schema  string
	sql     string
	scanned []string
	skip    bool
	ddlType int
}

func newDdlParser(sql, schema string) (dp *ddlParser, err error) {
	dp = new(ddlParser)
	dp.sql = sqlfilter(sql)
	dp.schema = schema

	if len(sql) == 0 {
		return dp, errors.New(`empty sql`)
	}

	dp.scan()
	dp.getDdlType()

	return
}

func (dp *ddlParser) scan() {
	var s scanner.Scanner
	var tok rune

	s.Init(strings.NewReader(dp.sql))
	s.Error = func(s *scanner.Scanner, msg string) {
		//
	}

	for tok != scanner.EOF {
		tok = s.Scan()
		if len(s.TokenText()) > 0 {
			dp.scanned = append(dp.scanned, s.TokenText())
		}
	}
}

func (dp *ddlParser) getDdlType() {
	tokens := dp.scanned[:2]

	for _, check := range ignoreOne {
		if up(tokens[0]) == check {
			dp.skip = true
			return
		}
	}

	for _, check := range ignoreTwo {
		if up(tokens[0])+" "+up(tokens[1]) == check {
			dp.skip = true
			return
		}
	}

	var wordsCount int

	for i, check := range have {
		wordsCount = len(strings.Split(check, ` `))

		if (wordsCount > 1 && up(tokens[0])+" "+up(tokens[1]) == check) || (wordsCount == 1 && up(tokens[0]) == check) {
			dp.skip = false
			dp.ddlType = i
			return
		}
	}

	dp.skip = false
	dp.ddlType = -1
}

func (dp *ddlParser) getTypeStruct() interface{} {
	switch dp.ddlType {
	case createTable:
		table, err := createTableScan(dp.scanned, dp.schema)
		if err != nil {
			return err
		}
		return table
	case createDatabase, createSchema:
		schema, err := newSchema(dp.scanned)
		if err != nil {
			return err
		}
		return schema
	case truncate:
		return truncateScan(dp.scanned, dp.schema)
	case renameTable:
		return renameScan(dp.scanned, dp.schema)
	case dropTable:
		return dropScan(dp.scanned, dp.schema)
	case alterTable:
		t, err := alterTableScan(dp.scanned, dp.schema)
		if err != nil {
			return err
		}

		if len(t.GetAddColumns()) == 0 && len(t.GetDropColumns()) == 0 && len(t.GetAddConstraints()) == 0 {
			return nil
		}
		return t
	}

	return nil
}

// return one of create table struct for create table sql
func createTableScan(cre []string, schema string) (create interface{}, err error) {
	//cut create table
	cre = cre[2:]

	//cut if not exists
	if up(cre[0]) == "IF" && up(cre[1]) == "NOT" {
		cre = cre[3:]
	}

	var tabledef isql.Table
	//check schema name
	if cre[1] == `.` {
		tabledef = isql.Table{Schema: tr(cre[0]), Name: tr(cre[2])}
		cre = cre[3:]
	} else {
		tabledef = isql.Table{Schema: schema, Name: tr(cre[0])}
		cre = cre[1:]
	}

	//check like
	if (cre[0] == "(" && up(cre[1]) == "LIKE") || up(cre[0]) == "LIKE" {
		//RETURN LIKE
		tablelike := isql.CreateTableLike{Table: tabledef}
		var like isql.Table
		cre = cre[1:]

		if up(cre[0]) == "LIKE" {
			cre = cre[1:]
		}

		if cre[1] == "." {
			like = isql.Table{Schema: tr(cre[0]), Name: tr(cre[2])}
			cre = cre[3:]
		} else {
			like = isql.Table{Schema: schema, Name: tr(cre[0])}
			cre = cre[1:]
		}

		tablelike.LikeTable = like

		return tablelike, err
	}

	//check create AS for example
	if cre[0] != "(" {
		return create, errors.New("not appliable create table")
	}

	tablecr := isql.CreateTable{Table: tabledef}

	cre = cre[1:]

	bracketsstate := 0

	var line [][]string
	var temp []string

	for i, val := range cre {
		//end column or key, go to next
		if val == "," && bracketsstate == 0 {
			line = append(line, temp)
			temp = []string{}
			continue
		}
		//last column or key
		if val == ")" && bracketsstate == 0 {
			line = append(line, temp)
			cre = cre[i+1:]
			break
		}

		if val == "(" {
			bracketsstate++
		}

		if val == ")" && bracketsstate > 0 {
			bracketsstate--
		}

		temp = append(temp, val)
	}

	for _, oneline := range line {
		switch lv := scanCreateLine(oneline).(type) {
		case isql.Constraint:
			tablecr.Constraints = append(tablecr.Constraints, lv)
		case isql.Column:
			tablecr.Columns = append(tablecr.Columns, lv)
		}
	}

	return tablecr, err
}

// return rename struct for rename sql
func renameScan(scanned []string, schema string) []isql.RenameTable {
	ren := []isql.RenameTable{}

	//cut RENAME TABLE
	scanned = scanned[2:]

	for i, val := range scanned {
		//find word TO (no table name check)
		if i%2 == 1 && up(val) == "TO" {
			var t isql.RenameTable

			tempT := isql.Table{}
			//get table before TO
			if i > 2 && scanned[i-2] == "." {
				//if schema exist
				tempT.Name = tr(scanned[i-1])
				tempT.Schema = tr(scanned[i-3])
			} else {
				tempT.Name = tr(scanned[i-1])
				tempT.Schema = schema
			}
			tempTto := isql.Table{}
			//get table after word TO
			if i+2 < len(scanned) && scanned[i+2] == "." {
				tempTto.Name = tr(scanned[i+3])
				tempTto.Schema = tr(scanned[i+1])
			} else {
				tempTto.Name = tr(scanned[i+1])
				tempTto.Schema = schema
			}

			t.From = tempT
			t.To = tempTto
			ren = append(ren, t)
		}
	}

	return ren
}

// return truncate struct for truncate sql
func truncateScan(scanned []string, schema string) isql.TruncateTable {
	trunc := isql.TruncateTable{}

	//cut TRUNCATE TABLE
	if strings.ToUpper(scanned[1]) == "TABLE" {
		scanned = scanned[2:]
	} else {
		scanned = scanned[1:]
	}

	if len(scanned) > 2 && scanned[1] == "." {
		trunc.Schema = tr(scanned[0])
		trunc.Name = tr(scanned[2])
	} else {
		trunc.Schema = schema
		trunc.Name = tr(scanned[0])
	}

	return trunc
}

// return drop struct for drop sql
func dropScan(scanned []string, schema string) []isql.DropTable {
	drops := []isql.DropTable{}

	//cut DROP TABLE
	scanned = scanned[2:]

	if up(scanned[0]) == "IF" && up(scanned[1]) == "EXISTS" {
		scanned = scanned[2:]
	}

	//check for schemas name
	tableGet := func(s []string, i int, schema string) isql.DropTable {
		tempT := isql.DropTable{}
		if (i-2 > 0) && (s[i-2] == ".") {
			tempT.Schema = tr(s[i-3])
			tempT.Name = tr(s[i-1])
		} else {
			tempT.Schema = schema
			tempT.Name = tr(s[i-1])
		}
		return tempT
	}

	for i, val := range scanned {
		//if more than one table drop
		if val == "," {
			tempT := tableGet(scanned, i, schema)
			drops = append(drops, tempT)
		}
	}

	drops = append(drops, tableGet(scanned, len(scanned), schema))

	return drops
}

const (
	primary = "primary"
	unique  = "unique"
)

// return key or column struct for create table column and key inner sql
func scanCreateLine(line []string) interface{} {
	keydef := []string{
		"CONSTRAINT",
		"PRIMARY",
		"UNIQUE",
		"FOREIGN",
		"INDEX",
		"KEY",
		"FULLTEXT",
		"SPATIAL",
		"CHECK"}

	isKey := false

	for _, val := range keydef {
		if val == up(line[0]) {
			//this is key
			isKey = true
			break
		}
	}

	if !isKey {
		t := isql.Column{Name: tr(line[0]), Type: line[1]}
		// if type with brackets
		if len(line) > 2 && line[2] == "(" {
			i := 2
			for line[i] != ")" {
				t.Type += line[i]
				i++
			}

			t.Type += ")"
		}

		return t
	}

	t := isql.Constraint{}
	if up(line[0]) == "PRIMARY" {
		t.Type = primary
	} else if up(line[0]) == "UNIQUE" {
		t.Type = unique
	} else {
		return nil
	}

	start := false
	//for type in keys with parenthesis like varchar(150)
	parenthesis := false

	for _, val := range line {
		if val == "(" && !start {
			start = true
			continue
		} else if val == "(" && start {
			parenthesis = true
			continue
		}

		if val == ")" && !parenthesis {
			start = false
			continue
		} else if val == ")" && parenthesis {
			parenthesis = false
			continue
		}

		if parenthesis {
			continue
		}

		if start && val != "," {
			t.Columns = append(t.Columns, tr(val))
		}
	}

	return t
}

// return create schema struct for create schema sql
func newSchema(schemaScan []string) (schema isql.CreateSchema, err error) {
	if len(schemaScan) < 3 {
		return schema, errors.New("wrong schema sql")
	}

	schemaName := tr(schemaScan[2])

	if schemaName == "" {
		err = errors.New("empty schema name")
	}

	schemas := isql.CreateSchema{}
	schemas.Name = schemaName

	return schemas, err
}

const (
	add = iota
	drop
	ignore
	exception
)

func alterTableScan(cre []string, schema string) (alteri isql.AlterTable, err error) {
	//cut alter table
	cre = cre[2:]

	var tabledef isql.Table
	//check schema name
	if cre[1] == "." {
		tabledef = isql.Table{Schema: tr(cre[0]), Name: tr(cre[2])}
		cre = cre[3:]
	} else {
		tabledef = isql.Table{Schema: schema, Name: tr(cre[0])}
		cre = cre[1:]
	}

	var alter isql.AlterTable

	alter.Table = tabledef

	bracketsState := 0

	var line [][]string
	var temp []string

	for _, val := range cre {
		//end column or key, go to next
		if val == "," && bracketsState == 0 {
			line = append(line, temp)
			temp = []string{}
			continue
		}

		if val == "(" {
			bracketsState++
		}

		if val == ")" && bracketsState > 0 {
			bracketsState--
		}

		temp = append(temp, val)
	}

	line = append(line, temp)

	for _, ll := range line {
		l, atype := scanAlterExp(ll)

		switch atype {
		case ignore:
			continue
		case exception:
			return alter, errors.New("Non supported ALTER")
		}

		switch lv := l.(type) {
		case isql.Constraint:
			if atype == add && lv.GetType() != "" {
				alter.AddConstraints = append(alter.AddConstraints, lv)
			}
		case isql.Column:
			if atype == add {
				alter.AddColumns = append(alter.AddColumns, lv)
			} else {
				alter.DropColumns = append(alter.DropColumns, lv)
			}
		}

	}

	return alter, err
}

func scanAlterExp(line []string) (val interface{}, atype int) {
	//ignores
	if up(line[0]) == "ALTER" || up(line[0]) == "ALGORITHM" || up(line[0]) == "LOCK" {
		return val, ignore
	}

	//can't ignore, need human operator
	if up(line[0]) == "MODIFY" || up(line[0]) == "CHANGE" {
		return val, exception
	}

	if up(line[0]) == "ADD" {
		//if after or first, we wan't it deal auto
		if up(line[len(line)-2]) == "AFTER" || up(line[len(line)-1]) == "FIRST" {
			return val, exception
		}

		if up(line[1]) == "COLUMN" {
			line = line[2:]
		} else {
			line = line[1:]
		}

		k := scanCreateLine(line)

		return k, add
	}

	if up(line[0]) == "DROP" && up(line[1]) != "INDEX" && up(line[1]) != "KEY" && up(line[1]) != "FOREIGN" &&
		up(line[1]) != "PRIMARY" && up(line[1]) != "PARTITION" {

		if up(line[1]) == "COLUMN" {
			line = line[2:]
		} else {
			line = line[1:]
		}

		k := isql.Column{Name: tr(line[0])}

		return k, drop
	}

	return val, ignore
}
