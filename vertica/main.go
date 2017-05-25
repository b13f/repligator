package vertica

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	//include ODBC driver
	_ "github.com/alexbrainman/odbc"

	"github.com/b13f/repligator/ddlparser"
	"github.com/b13f/repligator/isql"
)

//Config is vertica server credentials and other params
type Config struct {
	Odbc       string
	Host       string
	Port       string
	User       string
	Password   string
	Database   string
	Pack       int
	FlushCount int    `yaml:"flush_count"`
	FlushTime  int    `yaml:"flush_time"`
	DataDir    string `yaml:"data_dir"`
}

//Cache is main struct to store cached events and vertica server params
type Cache struct {
	sync.Mutex
	ODBCdsn    string
	db         *sql.DB
	tx         *sql.Tx
	tables     map[string]tableCache
	gtidSet    map[string]string
	delPack    int
	infoCache  string
	dataDir    string
	flushCount int
	flushTime  int
}

//Init create vertica destination connection and return connect
func Init(conf Config) (vertica *Cache, err error) {
	vertica = new(Cache)
	vertica.ODBCdsn = `Driver=` + conf.Odbc + `;Servername=` + conf.Host +
		`;Database=` + conf.Database + `;Port=` + conf.Port +
		`;uid=` + conf.User + `;pwd=` + conf.Password + `;`

	vertica.tables = make(map[string]tableCache)
	vertica.gtidSet = make(map[string]string)
	if vertica.delPack = conf.Pack; vertica.delPack == 0 {
		vertica.delPack = 5000
	}
	if conf.DataDir != "" {
		vertica.dataDir = strings.TrimRight(conf.DataDir, string(os.PathSeparator)) + string(os.PathSeparator)
	}

	vertica.flushCount = conf.FlushCount
	vertica.flushTime = conf.FlushTime

	err = vertica.checkRequirements()

	return vertica, err
}

func (vc *Cache) checkRequirements() (err error) {
	if _, err = os.Stat(vc.dataDir + `rejected`); os.IsNotExist(err) {
		if err = os.Mkdir(vc.dataDir+`rejected`, 0766); err != nil {
			return
		}
	}

	if _, err = os.Stat(vc.dataDir + `exceptions`); os.IsNotExist(err) {
		if err = os.Mkdir(vc.dataDir+`exceptions`, 0766); err != nil {
			return
		}
	}

	err = vc.getDb()

	if err == nil {
		createSQL := `CREATE TABLE IF NOT EXISTS public."__repligator_pos" (name VARCHAR(1024),gtid VARCHAR(1024),"timestamp" TIMESTAMPTZ, PRIMARY KEY (name)) ORDER BY name`
		_, err = vc.db.Exec(createSQL)
	}

	return
}

//GetHTTPInterfaces return http handlers
func (vc *Cache) GetHTTPInterfaces(skip chan string) map[string]func(w http.ResponseWriter, r *http.Request) {
	ret := make(map[string]func(w http.ResponseWriter, r *http.Request))

	ret[`/skip`] = func(w http.ResponseWriter, r *http.Request) {
		if vc.isCacheExist() {
			fmt.Fprint(w, `Can not skip transaction! Cache is not cleared.`)
			return
		}

		select {
		case info := <-skip:
			log.Infof(`Transaction: %s skipped`, info)
			fmt.Fprintf(w, `Transaction: %s skipped`, info)
		default:
			fmt.Fprint(w, `Nothing to skip`)
		}

	}

	ret[`/info`] = func(w http.ResponseWriter, r *http.Request) {
		//info := <-skip
		if len(vc.infoCache) != 0 {
			fmt.Fprintf(w, "%s", vc.infoCache)
		} else {
			fmt.Fprintf(w, "%s", vc.GetTablesCacheInfo(false))
		}
	}

	return ret
}

//GetBotInterfaces return handlers for bot realisations
func (vc *Cache) GetBotInterfaces(skip chan string) map[string]func(msg string) string {
	ret := make(map[string]func(msg string) string)

	ret[`skip`] = func(msg string) string {
		if vc.isCacheExist() {
			return `Can not skip transaction! Cache is not cleared.`
		}

		select {
		case info := <-skip:
			log.Infof(`Transaction: %s skipped`, info)
			return fmt.Sprintf(`Transaction: %s skipped`, info)
		default:
			return `Nothing to skip`
		}
	}

	ret[`vsql`] = func(msg string) string {
		if err := vc.clearCache(); err != nil {
			return fmt.Sprintf(`Clear error: %s`, err.Error())
		}

		if _, err := vc.Exec([]string{strings.Trim(msg[strings.Index(msg, `vsql`)+5:], `;`)}); err != nil {
			return fmt.Sprintf(`Vsql error: %s`, err.Error())
		}

		return `vsql done`
	}

	return ret
}

//ApplyEvent receive events to store in vertica
func (vc *Cache) ApplyEvent(receiver chan interface{}, skip chan string) chan error {
	var replicationEvent interface{}
	fatalError := make(chan error)

	var counter int
	var start = time.Now()
	counterReset := func() {
		dur := time.Since(start)
		log.Infof(`%d transactions done for %v`, counter, dur)
		start = time.Now()
		counter = 0
	}

	var err error
	go func() {
	MainLoop:
		for {
			select {
			case replicationEvent = <-receiver:
			case <-time.After(time.Second * 10):
				replicationEvent = nil
			}

			switch event := replicationEvent.(type) {
			case isql.RowsEvent:
				if err = vc.setRows(event); err != nil {
					log.Errorf(`Set rows error: %s`, err.Error())
					fatalError <- err
				}
				counter++
			case isql.DdlEvent:
				//ddl
				if counter > 0 {
					if err = vc.clearCache(); err != nil {
						log.Errorf(`Clear cache error: %s`, err.Error())
						fatalError <- err
					}

					counterReset()
				}

				var vsql []string
				if vsql, err = vc.getDDLFromEvent(event); err == nil && len(vsql) > 0 {
					_, err = vc.Exec(vsql)
					log.Debugf("DDL: %v", vsql)
				}

				//wait skip if some errors
				if err != nil {
					log.Warnf("Error: %s Vsql: %s Real sql %s", err.Error(), vsql, event.GetQuery())
					skip <- event.GetQuery()
				}

				//write position of current ddl
				vc.Lock()
				t := vc.gtidSet
				t[event.GetSourceName()] = event.GetGtidSet()
				vc.gtidSet = t
				vc.Unlock()

				if err = vc.flushPosition(); err != nil {
					log.Warnf(`Set pos error: %s`, err.Error())
				}

				continue
			case bool:
				break MainLoop
			case nil:
			}

			if counter == vc.flushCount || (time.Since(start).Seconds() > float64(vc.flushTime) && counter > 0) {
				if err = vc.flushCache(); err != nil {
					log.Errorf(`Flush error: %s`, err.Error())

					f, _ := os.OpenFile(vc.dataDir+`debug`, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
					f.Close()

					fatalError <- err
				}

				counterReset()
			}

		}
	}()

	return fatalError
}

func (vc *Cache) getDDLFromEvent(event isql.DdlEvent) (vsql []string, err error) {
	switch ddl := ddlparser.Ddlcase(event.GetQuery(), event.GetSchema()).(type) {
	case isql.CreateSchema:
		vsql = vc.getSchemaSQL(ddl)
	case isql.CreateTable:
		vsql = vc.GetTableSQL(ddl)
	case isql.CreateTableLike:
		vsql = vc.getTableLikeSQL(ddl)
	case []isql.RenameTable:
		vsql = vc.getRenameSQL(ddl)
	case isql.AlterTable:
		vsql, err = vc.getAlterSQL(ddl)
	case isql.TruncateTable:
		vsql = vc.getTruncateSQL(ddl)
	case []isql.DropTable:
		vsql = vc.GetDropSQL(ddl)
	case error:
		err = ddl
	case nil:
		return
	default:
		err = fmt.Errorf(`DDL case not found fot query: %s`, event.GetQuery())
	}

	return
}

//GetLastPosition return existed gtid set in vsql if exist
func (vc *Cache) GetLastPosition(name string) (gtid string, err error) {
	currentGTIDSql := `SELECT "gtid" FROM public.__repligator_pos WHERE name='%s'`

	rows, err := vc.db.Query(fmt.Sprintf(currentGTIDSql, name))
	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&gtid); err != nil {
			return
		}
	}

	return
}

//Exec run vertica sql
func (vc *Cache) Exec(vsqls []string) (aff int64, err error) {
	var res sql.Result

	for _, vsql := range vsqls {
		if res, err = vc.forceExec(vsql); err != nil {
			return
		}

		if aff, err = res.RowsAffected(); err != nil {
			return
		}

		if aff == 0 {
			log.Warnf("Affected 0: %s", vsql[:40])
		}
	}

	return
}

func (vc *Cache) getDb() (err error) {
	if vc.db, err = sql.Open("odbc", vc.ODBCdsn); err != nil {
		return
	}
	// no need to have more then one connection
	vc.db.SetMaxOpenConns(1)

	return
}

//GetTablesCacheInfo return current state of cache
func (vc *Cache) GetTablesCacheInfo(debug bool) string {
	vc.Lock()
	defer vc.Unlock()

	var out string

	for name, set := range vc.gtidSet {
		out += fmt.Sprintf("gtid: [%s] %s\n", name, set)
	}

	tpl := "\n Table: %s\n DELS: %d\n INS: %d\n"

	tplExt := " Columns: %s\n Enums: %d\n mConstr: %q\n mKeySort: %q\n"

	t := make(map[string]int)

	for _, table := range vc.tables {
		if len(table.tDels) > 1000 {
			t[table.schema+`.`+table.name] = len(table.tDels)
		}

		out += fmt.Sprintf(tpl, table.schema+`.`+table.name, len(table.tDels), len(table.tIns))

		if debug {
			out += fmt.Sprintf("\n dels: %+v \n ins: %+v\n", table.tDels, table.tIns)
		}

		out += fmt.Sprintf(tplExt, strings.Join(table.columnNames, "|"), len(table.enums), table.leadConstrColNames, table.leadConstrColOrder)
	}

	if len(t) > 0 {
		kk := "(ALERT!!) MAX DELS: \n"
		for table, cnt := range t {
			kk += fmt.Sprintf(" %s : %d\n", table, cnt)
		}
		out = kk + out
	}

	return out
}

func (vc *Cache) isCacheExist() bool {
	vc.Lock()
	ex := len(vc.tables) > 0
	vc.Unlock()
	return ex
}

func (vc *Cache) flushCacheExec() (err error) {
	vc.analyze()
	if err = vc.startTx(); err != nil {
		return
	}
	for i, table := range vc.tables {
		if err = table.tableDeletesExec(vc); err != nil {
			return
		}
		if err = table.tableInsertsExec(vc); err != nil {
			return
		}

		vc.tables[i] = table
	}
	if err = vc.flushPosition(); err != nil {
		return
	}
	if err = vc.commitTx(); err != nil {
		return
	}

	return
}

//flush & clear tables cache
func (vc *Cache) clearCache() (err error) {
	if err = vc.flushCache(); err != nil {
		return
	}
	vc.Lock()
	vc.tables = make(map[string]tableCache)
	vc.Unlock()
	return
}

//write tables data
func (vc *Cache) flushCache() (err error) {
	vc.infoCache = vc.GetTablesCacheInfo(false)

	vc.Lock()
	defer vc.Unlock()

	if err = vc.flushCacheExec(); err != nil {
		return
	}
	vc.infoCache = ""

	return
}

func (vc *Cache) analyze() (err error) {
	var queries []string

	for _, table := range vc.tables {
		queries = append(queries, table.analyzeStatisticsQuery())
	}

	_, err = vc.Exec(queries)

	return
}

func (vc *Cache) startTx() (err error) {
	vc.tx, err = vc.db.Begin()
	return
}

func (vc *Cache) commitTx() (err error) {
	if vc.tx != nil {
		err = vc.tx.Commit()

		if err == nil {
			vc.tx = nil
		}
	}
	return
}

func (vc *Cache) forceExec(expression string) (result sql.Result, err error) {
	if vc.tx != nil {
		result, err = vc.tx.Exec(expression)
	} else {
		result, err = vc.db.Exec(expression)
	}

	if err == sql.ErrTxDone {
		vc.tx = nil
		result, err = vc.db.Exec(expression)
	}

	return
}

//return enum values (if exists) from table comment
func (vc *Cache) getTableEnumValues(schema, table string) (enums []enum, err error) {
	var serializedEnums string

	_ = vc.db.QueryRow(fmt.Sprintf(getEnumsTmpl, schema, table)).Scan(&serializedEnums)

	if len(serializedEnums) == 0 {
		return
	}

	for _, field := range strings.Split(serializedEnums, `;`) {
		enumF := new(enum)
		enumF.deserialize(field)
		enums = append(enums, *enumF)
	}

	return
}

var updateSQL = `UPDATE public."__repligator_pos" SET gtid='%s',"timestamp"=NOW() WHERE name='%s'`
var insertSQL = `INSERT INTO public."__repligator_pos"(name,gtid,"timestamp") VALUES ('%s','%s',NOW())`

//write saved transaction gtid in vsql destination
func (vc *Cache) flushPosition() (err error) {
	var res sql.Result
	var aff int64

	for sourceName, set := range vc.gtidSet {
		if res, err = vc.forceExec(fmt.Sprintf(updateSQL, set, sourceName)); err != nil {
			return
		}

		if aff, err = res.RowsAffected(); err != nil {
			return err
		}

		if aff == 0 {
			if _, err = vc.forceExec(fmt.Sprintf(insertSQL, sourceName, set)); err != nil {
				return err
			}
		}
	}

	return
}
