package vertica

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/b13f/repligator/isql"
)

var roundBrack = regexp.MustCompile(`\([0-9]+\)`)

//return schema create vsql
func (vc *Cache) getSchemaSQL(schema isql.CreateSchema) []string {
	sqlTmpl := `CREATE SCHEMA IF NOT EXISTS "%s"`

	vsql := fmt.Sprintf(sqlTmpl, schema.GetName())

	return []string{vsql}
}

//GetTableSQL return create table statement in vsql
func (vc *Cache) GetTableSQL(ddl isql.CreateTable) (sqls []string) {
	sqlCreateTmpl := `CREATE TABLE IF NOT EXISTS "%s"."%s"` + "\n(\n" + `%s) %s`
	sqlSegmTmpl := ` SEGMENTED BY hash("%s") ALL NODES`
	columnTmpl := `"%s" %s,` + "\n"
	columns := ``
	order := ``
	//store enum values into comment
	var enums []string

	for i, col := range ddl.GetColumns() {
		columns += fmt.Sprintf(columnTmpl, col.GetName(), vc.typeConvert(col.GetType()))
		//enum check
		if enumReg.MatchString(col.GetType()) {
			enums = append(enums, serializeEnum(col.GetType(), i+1))
		}
	}

	for _, key := range ddl.GetConstraints() {
		if key.GetType() == isql.Primary {
			columns += `PRIMARY KEY ("` + strings.Join(key.GetColumns(), `","`) + "\") ENABLED,\n"
			order = fmt.Sprintf(`ORDER BY "%s"`, strings.Join(key.GetColumns(), `","`))
		}

		if key.GetType() == isql.Unique {
			var columnsNames []string
			for _, column := range key.GetColumns() {
				columnsNames = append(columnsNames, roundBrack.ReplaceAllLiteralString(column, ""))
			}

			//when primary not exist segment projection
			if len(order) == 0 {
				order = fmt.Sprintf(`ORDER BY "%s"`, strings.Join(columnsNames, `","`))
				order += fmt.Sprintf(sqlSegmTmpl, strings.Join(columnsNames, `","`))
			}

			columns += `UNIQUE ("` + strings.Join(columnsNames, `","`) + "\") ENABLED,\n"
		}
	}

	columns = strings.Trim(columns, ",\n")

	sqls = append(sqls, fmt.Sprintf(sqlCreateTmpl, ddl.GetCreateTable().GetSchema(), ddl.GetCreateTable().GetName(), columns, order))

	if len(enums) > 0 {
		sqls = append(sqls, setEnumSQL(ddl.GetCreateTable().GetSchema(), ddl.GetCreateTable().GetName(), enums))
	}

	return
}

func (vc *Cache) getTableLikeSQL(ddl isql.CreateTableLike) (sqls []string) {
	sqlTmpl := `CREATE TABLE IF NOT EXISTS "%s"."%s" LIKE "%s"."%s"`

	sqls = append(sqls, fmt.Sprintf(sqlTmpl, ddl.GetTable().GetSchema(), ddl.GetTable().GetName(), ddl.GetLikeTable().GetSchema(), ddl.GetLikeTable().GetName()))

	return
}

func (vc *Cache) getTruncateSQL(truncate isql.TruncateTable) (vsqls []string) {
	vsqlTmpl := `TRUNCATE TABLE "%s"."%s"`

	vsqls = append(vsqls, fmt.Sprintf(vsqlTmpl, truncate.GetSchema(), truncate.GetName()))

	return
}

func (vc *Cache) getRenameSQL(renames []isql.RenameTable) (vsqls []string) {
	//because projections not renamed and rise conflicts, create new table and drop old
	vsqlTmplCreate := `CREATE TABLE IF NOT EXISTS "%s"."%s" AS SELECT * FROM "%s"."%s"`

	vsqlTmplDrop := `DROP TABLE IF EXISTS "%s"."%s" CASCADE`

	for _, r := range renames {
		vsqls = append(vsqls, fmt.Sprintf(vsqlTmplCreate, r.GetTo().GetSchema(), r.GetTo().GetName(), r.GetFrom().GetSchema(), r.GetFrom().GetName()))
		vsqls = append(vsqls, fmt.Sprintf(vsqlTmplDrop, r.GetFrom().GetSchema(), r.GetFrom().GetName()))
	}

	return
}

//GetDropSQL return drop statement
func (vc *Cache) GetDropSQL(drops []isql.DropTable) (vsqls []string) {
	vsqlTmplSame := `DROP TABLE IF EXISTS "%s"."%s" CASCADE`

	for _, t := range drops {
		vsqls = append(vsqls, fmt.Sprintf(vsqlTmplSame, t.GetSchema(), t.GetName()))
	}

	return
}

//return alter table statement in vsql
func (vc *Cache) getAlterSQL(ddl isql.AlterTable) (sqls []string, err error) {

	alter := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, ddl.GetAlterTable().GetSchema(), ddl.GetAlterTable().GetName())

	columnAddTmpl := `ADD COLUMN "%s" %s`
	columnDropTmpl := `DROP COLUMN "%s" CASCADE`

	for _, col := range ddl.GetDropColumns() {
		sqls = append(sqls, alter+fmt.Sprintf(columnDropTmpl, col.GetName()))
	}

	for _, col := range ddl.GetAddColumns() {
		sqls = append(sqls, alter+fmt.Sprintf(columnAddTmpl, col.GetName(), vc.typeConvert(col.GetType())))
	}

	vc.tables = make(map[string]tableCache)

	for _, key := range ddl.GetAddConstraints() {
		if key.GetType() == isql.Primary {
			sqls = append(sqls, alter+`ADD PRIMARY KEY ("`+strings.Join(key.GetColumns(), `","`)+`") ENABLED`)
		}

		if key.GetType() == isql.Unique {
			var columnsNames []string
			for _, column := range key.GetColumns() {
				columnsNames = append(columnsNames, roundBrack.ReplaceAllLiteralString(column, ""))
			}

			sqls = append(sqls, alter+`ADD UNIQUE ("`+strings.Join(columnsNames, `","`)+`") ENABLED`)
		}
	}

	enumsSQL, err := vc.alterEnumsChecks(ddl)
	if err != nil {
		return
	}

	sqls = append(sqls, enumsSQL...)

	return
}

func (vc *Cache) alterEnumsChecks(ddl isql.AlterTable) (sqls []string, err error) {
	tableInfo, err := vc.newVerticaTableCache(ddl.GetAlterTable().GetSchema(), ddl.GetAlterTable().GetName())

	if err != nil {
		return
	}

	enumsToDel := make(map[int64]bool)
	var enumDeletedPos []int64
	var enums []string

	for i, col := range ddl.GetAddColumns() {
		if enumReg.MatchString(col.GetType()) {
			enumPos := len(tableInfo.columnNames) - len(ddl.GetDropColumns()) + 1 + i
			enums = append(enums, serializeEnum(col.GetType(), enumPos))
		}
	}

	if len(tableInfo.enums) == 0 {
		sqls = append(sqls, setEnumSQL(ddl.GetAlterTable().GetSchema(), ddl.GetAlterTable().GetName(), enums))
		return
	}

	for _, col := range ddl.GetDropColumns() {
		//get columns name
		for i, colname := range tableInfo.columnNames {
			//if column del
			if colname == col.GetName() {
				enumsToDel[int64(i+1)] = true
				enumDeletedPos = append(enumDeletedPos, int64(i+1))
				break
			}
		}
	}

	for _, enum := range tableInfo.enums {
		if !enumsToDel[enum.column] {
			newEnumPos := enum.column

			for _, delPos := range enumDeletedPos {
				if delPos < enum.column {
					newEnumPos--
				}
			}

			enum.column = newEnumPos
			enums = append(enums, enum.serialize())
		}
	}

	sqls = append(sqls, setEnumSQL(ddl.GetAlterTable().GetSchema(), ddl.GetAlterTable().GetName(), enums))
	return
}

var contains = map[string]string{
	"datetime":  "DATETIME",
	"timestamp": "TIMESTAMPTZ",
	"year":      "DECIMAL(4)",
	"text":      "VARCHAR(65000)",
	"blob":      "VARBINARY(65000)",
	"json":      "VARCHAR(65000)",
}

var sampleStarts = map[string]string{
	"date":      "DATE",
	"time":      "TIME",
	"tinyint":   "TINYINT",
	"smallint":  "SMALLINT",
	"mediumint": "INT",
	"int":       "INT",
	"bigint":    "NUMBER",
	"varbinary": "VARBINARY",
	"float":     "FLOAT",
	"double":    "DOUBLE PRECISION",
	"set":       "VARCHAR(4000)",
	"bit(1)$":   "BOOLEAN",
	"bit":       "CHAR(64)",
}

var numb = regexp.MustCompile("[0-9]+")

var funcStarts = map[string]func(string) string{
	"decimal": func(val string) string {
		return strings.ToUpper(val)
	},
	"varchar": func(val string) string {
		num := numb.FindAllString(val, 1)[0]
		if i64, _ := strconv.ParseInt(num, 10, 32); i64 > 65000 {
			return "VARCHAR(65000) /* WARN: long varchar*/"
		}
		return strings.ToUpper(val)
	},
	"char": func(val string) string {
		return strings.ToUpper(val)
	},
	"binary": func(val string) string {
		return strings.ToUpper(val)
	},
	"enum": func(val string) string {
		//TODO: not really nice
		return "VARCHAR(" + strconv.Itoa(len(val)) + ") /* ENUM: " + val + "*/"
	},
}

// converting mysql type in vertica type
func (vc *Cache) typeConvert(mysql string) (vsql string) {
	mtype := strings.ToLower(mysql)

	for search, val := range contains {
		if strings.Contains(mtype, search) {
			vsql = val
			return
		}
	}

	for search, val := range sampleStarts {
		if strings.HasPrefix(mtype, search) {
			vsql = val
			return
		}
	}

	for search, valFunc := range funcStarts {
		if strings.HasPrefix(mtype, search) {
			vsql = valFunc(mtype)
			return
		}
	}

	return
}
