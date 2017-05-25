package vertica

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type constraint struct {
	constraintType     string
	columnsPositionMap map[string]int
}

type tableCache struct {
	schema             string
	name               string
	columnNames        []string
	enums              []enum
	constraints        []constraint
	leadConstrColOrder []int
	leadConstrColNames map[string]int    //main constraint to operate
	tDels              []string          //values to del query
	tIns               map[string]string //values to csv copy query
}

var tableConstraintsSQLTmpl = `SELECT constraint_id,column_name,constraint_type FROM V_CATALOG.constraint_columns WHERE table_schema='%s' AND table_name='%s' AND constraint_type in ('p','u') ORDER BY constraint_id`

func (vc *Cache) newVerticaTableCache(schema, table string) (t tableCache, err error) {
	t = tableCache{schema: schema, name: table}

	if t.constraints, err = vc.getTableConstraints(schema, table); err != nil {
		return
	}

	tvsql := `SELECT column_name FROM columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position`

	rows, err := vc.db.Query(fmt.Sprintf(tvsql, schema, table))
	if err != nil {
		return
	}

	var columnName string

	// constraints and column names init
	for rows.Next() {
		if err = rows.Scan(&columnName); err != nil {
			return t, err
		}

		for i, constraint := range t.constraints {
			for constrColName, val := range constraint.columnsPositionMap {
				//if pos in key not finde yet, set the pos
				if constrColName == columnName && val == -1 {
					constraint.columnsPositionMap[columnName] = len(t.columnNames)
				}
			}
			t.constraints[i] = constraint
		}

		t.columnNames = append(t.columnNames, columnName)
	}

	rows.Close()

	//if table not exist
	if len(t.columnNames) == 0 {
		log.Infof("Table %s.%s not find in source", schema, table)
		return t, errors.New("Table not exist")
	}

	if t.enums, err = vc.getTableEnumValues(schema, table); err != nil {
		return
	}

	t.leadConstrColNames = t.mainConstrInit(t.constraints)

	var sortedKeys []int

	for _, pos := range t.leadConstrColNames {
		sortedKeys = append(sortedKeys, pos)
	}
	sort.Ints(sortedKeys)

	t.leadConstrColOrder = sortedKeys

	t.tIns = make(map[string]string)

	return t, nil
}

func (vc *Cache) getTableConstraints(schema, table string) (constraints []constraint, err error) {
	constraintsRows, err := vc.db.Query(fmt.Sprintf(tableConstraintsSQLTmpl, schema, table))

	if err != nil {
		return
	}

	defer constraintsRows.Close()

	var id, prevID int
	var columnName, constraintType string
	var tempConstraint constraint

	for constraintsRows.Next() {
		err = constraintsRows.Scan(&id, &columnName, &constraintType)
		if err != nil {
			return
		}

		if id != prevID {
			if prevID != 0 {
				constraints = append(constraints, tempConstraint)
			}

			tempConstraint = constraint{constraintType: constraintType}
			tempConstraint.columnsPositionMap = make(map[string]int)
			tempConstraint.columnsPositionMap[columnName] = -1
		} else {
			tempConstraint.columnsPositionMap[columnName] = -1
		}

		prevID = id
	}

	constraints = append(constraints, tempConstraint)

	return
}

func (t *tableCache) getRowHashKey(row []interface{}) string {
	//hash without collision
	return generateRow(row)
}

func (t *tableCache) analyzeStatisticsQuery() string {
	return fmt.Sprintf(`SELECT analyze_statistics('%s')`, t.schema+"."+t.name)
}

func (t *tableCache) mainConstrInit(constraints []constraint) (key map[string]int) {
	if len(constraints) == 0 {
		return
	}
	//check primary key
	for _, constr := range constraints {
		if constr.constraintType == "p" {
			key = constr.columnsPositionMap
			break
		}
	}
	//check unique key
	if len(key) == 0 {
		for _, constr := range constraints {
			if constr.constraintType == "u" {
				key = constr.columnsPositionMap
				break
			}
		}
	}
	return
}

func (t *tableCache) addIns(rows [][]interface{}) (err error) {
	for _, row := range rows {
		if len(t.enums) > 0 {
			enumToVal(t.enums, row)
		}

		hash := t.getRowHashKey(row)

		//check for collisions
		if val, ok := t.tIns[hash]; ok {
			if val != generateRowCopy(row) {
				log.Warnf("insert collision\n old: %s\n new: %s", val, generateRow(row))
			}
		}

		t.tIns[t.getRowHashKey(row)] = generateRowCopy(row)
	}

	return
}

func (t *tableCache) addDel(rows [][]interface{}) {
	for _, row := range rows {
		if len(t.enums) > 0 {
			enumToVal(t.enums, row)
		}

		hash := t.getRowHashKey(row)

		//first check in local inserts
		if _, ok := t.tIns[hash]; ok {
			delete(t.tIns, hash)
		} else {
			t.tDels = append(t.tDels, t.generateDel(row))
		}
	}
}

func (t *tableCache) generateDel(row []interface{}) string {
	//full del
	if len(t.leadConstrColNames) == 0 {
		sqlDelFull := `DELETE FROM "%s"."%s" WHERE %s`
		var val string
		var columnValue []string

		for i, column := range t.columnNames {
			val = generateRow(row[i : i+1])
			if val == "NULL" {
				columnValue = append(columnValue, fmt.Sprintf(`"%s" IS NULL`, column))
			} else {
				columnValue = append(columnValue, fmt.Sprintf(`"%s"=%s`, column, val))
			}
		}

		return fmt.Sprintf(sqlDelFull, t.schema, t.name, strings.Join(columnValue, " AND "))
	}

	//del by primary or unique
	curr := make([]interface{}, 0)
	for _, n := range t.leadConstrColOrder {
		curr = append(curr, row[n])
	}

	var valTpl string
	if valTpl = "(%s)"; len(t.leadConstrColNames) == 1 {
		valTpl = "%s"
	}

	return fmt.Sprintf(valTpl, generateRow(curr))
}

func (t *tableCache) getDelSQL(pack int) (vsqls []string) {
	if len(t.leadConstrColNames) == 0 {
		return t.tDels
	}

	delTpl := `DELETE FROM "%s"."%s" WHERE %s IN (%s)`

	var columnNames string

	keyNames := make([]string, 0)
	for _, n := range t.leadConstrColOrder {
		keyNames = append(keyNames, t.columnNames[n])
	}

	if len(t.leadConstrColNames) == 1 {
		columnNames = `"` + keyNames[0] + `"`
	} else {
		columnNames = `("` + strings.Join(keyNames, `","`) + `")`
	}

	for p := 0; p < ((len(t.tDels) / pack) + 1); p++ {
		if p < (len(t.tDels) / pack) {
			vsqls = append(vsqls, fmt.Sprintf(delTpl, t.schema, t.name, columnNames, strings.Join(t.tDels[(p*pack):((p+1)*pack)], ",")))
		} else {
			vsqls = append(vsqls, fmt.Sprintf(delTpl, t.schema, t.name, columnNames, strings.Join(t.tDels[(p*pack):], ",")))
		}
	}

	return
}

func (t *tableCache) getInsFile(dataDir string) (filename string, err error) {
	filename = dataDir + t.schema + `-` + t.name

	os.Remove(filename)

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}

	for _, val := range t.tIns {
		val = strings.Replace(val, "\t\r\n", "\t\n", -1)
		//pls use mysql NO_ZERODATES
		val = strings.Replace(val, `"0000-00-00 00:00:00"`, `NULL`, -1)
		if _, err = f.WriteString(val + "\t\r\n"); err != nil {
			return
		}
	}

	return
}

func (t *tableCache) tableDeletesExec(vert *Cache) (err error) {
	if len(t.tDels) == 0 {
		return
	}

	delVsql := t.getDelSQL(vert.delPack)

	var aff int64
	var tr int

	if tr = 220; tr >= len(delVsql[0]) {
		tr = len(delVsql[0])
	}

	log.Debugf("Start %d dels(packs: %d): %s", len(t.tDels), len(delVsql), delVsql[0][:tr])

	if aff, err = vert.Exec(delVsql); err != nil {
		return
	}

	if len(delVsql) == 1 && int(aff) != len(t.tDels) {
		log.Infof("AFFECTED DEL WRONG (del %d from %d): %s", aff, len(t.tDels), delVsql[0])
	}

	t.tDels = make([]string, 0)

	return
}

func (t *tableCache) tableInsertsExec(vert *Cache) (err error) {
	if len(t.tIns) == 0 {
		return
	}

	var filename string
	if filename, err = t.getInsFile(vert.dataDir); err != nil {
		return
	}

	copyTpl := `
		COPY "%s"."%s" FROM LOCAL '%s'
		DELIMITER ',' NULL AS 'NULL' ENCLOSED BY '"' RECORD TERMINATOR E'\t\r\n'
		REJECTED DATA '` + vert.dataDir + `rejected` + string(os.PathSeparator) + `%s.log'
		EXCEPTIONS '` + vert.dataDir + `exceptions` + string(os.PathSeparator) + `%s.log' ABORT ON ERROR NO COMMIT`

	copySQL := fmt.Sprintf(copyTpl, t.schema, t.name, filename, t.schema+t.name, t.schema+t.name)

	if _, err = vert.Exec([]string{copySQL}); err != nil {
		return
	}

	if err = os.Remove(filename); err != nil {
		return
	}

	t.tIns = make(map[string]string)

	return
}
