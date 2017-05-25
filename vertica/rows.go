package vertica

import (
	"fmt"
	"strings"

	"github.com/b13f/repligator/isql"
)

func (vc *Cache) tIns(schema, table string, rows [][]interface{}) (err error) {
	vc.Lock()
	defer vc.Unlock()
	vTableCache, ok := vc.tables[schema+table]

	if !ok {
		if vc.tables[schema+table], err = vc.newVerticaTableCache(schema, table); err != nil {
			return
		}
		vTableCache = vc.tables[schema+table]
	}

	err = vTableCache.addIns(rows)

	return
}

func (vc *Cache) tDel(schema, table string, rows [][]interface{}) (err error) {
	vc.Lock()
	defer vc.Unlock()
	vTableCache, ok := vc.tables[schema+table]

	if !ok {
		if vc.tables[schema+table], err = vc.newVerticaTableCache(schema, table); err != nil {
			return
		}
		vTableCache = vc.tables[schema+table]
	}

	vTableCache.addDel(rows)

	vc.tables[schema+table] = vTableCache

	return
}

func (vc *Cache) setRows(events isql.RowsEvent) (err error) {
	//for statement in transactions
	for _, e := range events.GetTables() {
		//for rows events in one query
		for _, rows := range e.GetRows() {
			var delRows, insRows [][]interface{}

			switch rows.GetType() {
			case isql.Insert:
				insRows = append(insRows, rows.GetValues()...)
			case isql.Delete:
				delRows = append(delRows, rows.GetValues()...)
			case isql.Update:
				for i, rows := range rows.GetValues() {
					if i%2 == 0 {
						delRows = append(delRows, rows)
					} else {
						insRows = append(insRows, rows)
					}
				}
			}

			if err = vc.tDel(e.GetTable().GetSchema(), e.GetTable().GetName(), delRows); err != nil {
				return
			}

			if err = vc.tIns(e.GetTable().GetSchema(), e.GetTable().GetName(), insRows); err != nil {
				return
			}
		}
	}

	vc.Lock()
	t := vc.gtidSet
	t[events.GetSourceName()] = events.GetGtidSet()
	vc.gtidSet = t
	vc.Unlock()

	return
}

// vsql values row from replica full row values
func generateRow(values []interface{}) string {
	var rowValues []string

	for _, d := range values {
		switch val := d.(type) {
		case string:
			rowValues = append(rowValues, `'`+strings.Replace(val, `'`, `''`, -1)+`'`)
		case int, int8, int32, int16, int64 /*ENUM!!!!*/ :
			rowValues = append(rowValues, fmt.Sprint(val))
		case float32, float64:
			rowValues = append(rowValues, fmt.Sprint(val))
			//tinyTEXT
		case []uint8:
			rowValues = append(rowValues, `'`+strings.Replace(bytesToString(val), `'`, `''`, -1)+`'`)
		case nil:
			rowValues = append(rowValues, "NULL")
		default:
			rowValues = append(rowValues, val.(string))
		}
	}

	return strings.Join(rowValues, ",")
}

// vsql values row for cvs file for COPY command from replica full row values
func generateRowCopy(values []interface{}) string {
	var rowValues []string

	for _, d := range values {
		switch val := d.(type) {
		case string:
			val = strings.Replace(val, `\`, `\\`, -1)
			val = strings.Replace(val, `"`, `\"`, -1)
			rowValues = append(rowValues, `"`+val+`"`)
		case int, int8, int32, int16, int64 /*ENUM!!!!*/ :
			rowValues = append(rowValues, fmt.Sprintf(`"%d"`, val))
		case float32, float64:
			rowValues = append(rowValues, fmt.Sprintf(`"%f"`, val))
			//tinyTEXT
		case []uint8:
			t := bytesToString(val)
			t = strings.Replace(t, `\`, `\\`, -1)
			t = strings.Replace(t, `"`, `\"`, -1)
			rowValues = append(rowValues, `"`+t+`"`)
		case nil:
			//null without quotes
			rowValues = append(rowValues, `NULL`)
		default:
			rowValues = append(rowValues, `"`+val.(string)+`"`)
		}
	}

	return strings.Join(rowValues, ",")
}

func bytesToString(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}
