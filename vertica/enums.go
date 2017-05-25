package vertica

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var enumSerializeForm = `enum(%d[%s])`

var enumReg = regexp.MustCompile(`(?i)enum(?: ?)\(([[:graph:]]+)\)`)

var getEnumsTmpl = `SELECT "comment" FROM V_CATALOG.COMMENTS WHERE object_type='TABLE' AND object_schema='%s' AND object_name='%s';`

var enumcomm = regexp.MustCompile(`^enum\(([0-9]+)\[([[:print:]]+)\]\)$`)

var setEnumTmpl = `COMMENT ON TABLE "%s"."%s" IS '%s'`

type enum struct {
	column int64
	values []string
}

func (e *enum) deserialize(value string) {
	values := enumcomm.FindAllStringSubmatch(value, -1)

	columnPosition, _ := strconv.ParseInt(values[0][1], 10, 32)
	e.column = columnPosition

	valuesEnum := strings.Split(values[0][2], `,`)
	e.values = e.values[:0]
	for _, value := range valuesEnum {
		e.values = append(e.values, strings.Trim(value, `"'`))
	}
}

func (e *enum) serialize() (value string) {
	return fmt.Sprintf(enumSerializeForm, e.column, `"`+strings.Join(e.values, `","`)+`"`)
}

func serializeEnum(columnType string, columnPos int) string {
	enumFields := enumReg.FindStringSubmatch(columnType)[1]
	return fmt.Sprintf(enumSerializeForm, columnPos, strings.Replace(enumFields, `'`, `''`, -1))
}

//return enum string value by position
func enumToVal(enums []enum, rows []interface{}) {
	for _, enum := range enums {
		switch t := rows[enum.column-1].(type) {
		case int64:
			if rows[enum.column-1] = ``; t != 0 {
				rows[enum.column-1] = enum.values[t-1]
			}
		}
	}
}

func setEnumSQL(schema, table string, enums []string) string {
	return fmt.Sprintf(setEnumTmpl, schema, table, strings.Join(enums, `;`))
}
