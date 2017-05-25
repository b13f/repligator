package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/johntdyer/slackrus"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"gopkg.in/yaml.v2"

	"github.com/b13f/repligator/ddlparser"
	"github.com/b13f/repligator/isql"
	"github.com/b13f/repligator/vertica"
)

const defaultTryAfter = 5

type config struct {
	Sources     []configSource
	Destination vertica.Config
	Port        string
	LogFile     string `yaml:"log_file"`
	LogLevel    string `yaml:"log_level"`
	Slack       struct {
		BotToken string `yaml:"bot_token"`
		Hook     string
		Channel  string
		Username string
		Icon     string
	}
}

type configSource struct {
	Name     string
	Type     string
	ServerID uint32 `yaml:"server_id"`
	Host     string
	Port     uint16
	User     string
	Password string
	Gtid     string
	Timeout  time.Duration
	TryAfter time.Duration `yaml:"try_after"`
	Schemas  []configSourceSchema
}

type configSourceSchema struct {
	Name          string
	TablesSync    []string `yaml:"sync"`
	TablesExclude []string `yaml:"exclude"`
	Gtid          string
}

var data config

var configFile = flag.String("config", "config.yml", "path to config file")
var dumpFolder = flag.String("df", ``, "path to dir with MySQL dump files")

func runDumpMutate() {
	out, err := getVsqlFromDir(*dumpFolder)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Print(out)
	os.Exit(0)
}

func configRead() {
	bytes, err := ioutil.ReadFile(*configFile)

	if err == nil {
		err = yaml.Unmarshal(bytes, &data)
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	if len(data.LogFile) > 0 {
		f, err := os.OpenFile(data.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			log.Fatalf("error opening file %s: %v", data.LogFile, err)
		}

		log.SetOutput(f)
	}

	if len(data.LogLevel) > 0 {
		level, err := log.ParseLevel(data.LogLevel)
		if err == nil {
			log.SetLevel(level)
		}
	}

	if data.Slack.Hook != "" {
		log.AddHook(&slackrus.SlackrusHook{
			HookURL:        data.Slack.Hook,
			AcceptedLevels: slackrus.LevelThreshold(log.WarnLevel),
			Channel:        data.Slack.Channel,
			IconEmoji:      data.Slack.Icon,
			Username:       data.Slack.Username,
		})
	}
}

func main() {
	flag.Parse()

	if len(*dumpFolder) > 0 {
		runDumpMutate()
	}

	configRead()

	eventsConnector := make(chan interface{})

	receiver, err := vertica.Init(data.Destination)

	if err != nil {
		log.Fatal(err.Error())
	}

	skip := make(chan string)

	cancel := make(chan configSource)

	//reconnect to source if errors
	go func() {
		for {
			canceled := <-cancel

			time.AfterFunc(time.Minute*canceled.TryAfter, func() {
				log.Infof("Reconnect to %s", canceled.Name)

				lastPosition, err := receiver.GetLastPosition(canceled.Name)

				if err != nil {
					log.Fatal(err.Error())
				}

				if len(lastPosition) > 0 {
					canceled.Gtid = lastPosition
				}

				listenSource(canceled, eventsConnector, cancel)
			})
		}
	}()

	for _, sourceConfig := range data.Sources {
		//check for existed position in source
		lastPosition, err := receiver.GetLastPosition(sourceConfig.Name)

		if err != nil {
			log.Fatal(err.Error())
		}

		if len(lastPosition) > 0 {
			sourceConfig.Gtid = lastPosition
		}

		if sourceConfig.TryAfter == 0 {
			sourceConfig.TryAfter = defaultTryAfter
		}

		go listenSource(sourceConfig, eventsConnector, cancel)
	}

	receiverError := receiver.ApplyEvent(eventsConnector, skip)

	//init bot
	if data.Slack.BotToken != "" {
		slackbot := initBot(data.Slack.BotToken)

		msgs := slackbot.receive()

		go func() {
			for msg := range msgs {
				if msg == "ping" {
					slackbot.send("pong")
					continue
				}

				for cmd, vfunc := range receiver.GetBotInterfaces(skip) {
					if strings.HasPrefix(msg, cmd) {
						slackbot.send(vfunc(msg))
						continue
					}
				}
			}
		}()
	}

	mux := http.NewServeMux()

	for path, vfunc := range receiver.GetHTTPInterfaces(skip) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			vfunc(w, r)
		})
	}

	s := &http.Server{
		Addr:         ":" + data.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		err := s.ListenAndServe()

		if err != nil {
			log.Fatal(err.Error())
		}
	}()

	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		err = <-receiverError
		panic(err.Error())
	}
}

func listenSource(src configSource, send chan interface{}, cancelSource chan configSource) {

	cfg := replication.BinlogSyncerConfig{
		ServerID: src.ServerID,
		Flavor:   src.Type,
		Host:     src.Host,
		Port:     src.Port,
		User:     src.User,
		Password: src.Password,
		LogLevel: "warn",
	}
	syncer := replication.NewBinlogSyncer(&cfg)

	gtid, err := mysql.ParseGTIDSet(src.Type, src.Gtid)

	if err != nil {
		log.Warn(err)
		cancelSource <- src
		return
	}

	streamer, err := syncer.StartSyncGTID(gtid)

	if err != nil {
		log.Warn(err)
		cancelSource <- src
		return
	}

	gtidSet := getGtidSet(src.Gtid)

	var rowsEvent isql.TableRowsEvent
	var rowsEvents []isql.TableRowsEvent

	for {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*src.Timeout)

		ev, err := streamer.GetEvent(ctx)

		if ctx.Err() != nil {
			syncer.Close()
			log.Warnf("timeout source, %s closed", src.Host)
			cancelSource <- src
			cancel()
			return
		}

		if err != nil {
			syncer.Close()
			log.Warnf("event error %s - %s : %s", src.Host, src.Name, err.Error())
			cancelSource <- src
			cancel()
			return
		}

		switch t := ev.Event.(type) {
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(t.SID)

			gtid, ok := gtidSet[u.String()]
			//if not existed gtid source
			if !ok {
				gtid.Start = fmt.Sprintf("%d", t.GNO)
			}

			gtid.Last = fmt.Sprintf("%d", t.GNO)

			gtidSet[u.String()] = gtid
		case *replication.QueryEvent:
			switch string(t.Query) {
			case `BEGIN`:
				rowsEvents = []isql.TableRowsEvent{}
			case `COMMIT`:
				continue
			default:
				//TODO: get table and schema for ddl
				send <- isql.DdlEvent{
					SourceName: src.Name,
					Schema:     string(t.Schema),
					Query:      string(t.Query),
					GtidSet:    gtidSetToString(gtidSet),
				}
			}
		case *replication.RowsEvent:
			tempRows := isql.Rows{}

			for _, row := range t.Rows {
				tempRows.Values = append(tempRows.Values, row)
			}

			switch ev.Header.EventType.String() {
			case "WriteRowsEventV2", "WriteRowsEventV1":
				tempRows.Type = isql.Insert
			case "UpdateRowsEventV2", "UpdateRowsEventV1":
				tempRows.Type = isql.Update
			case "DeleteRowsEventV2", "DeleteRowsEventV1":
				tempRows.Type = isql.Delete
			default:
				continue
			}

			rowsEvent.Rows = append(rowsEvent.Rows, tempRows)
		case *replication.TableMapEvent:
			if len(rowsEvent.Table.GetName()) > 0 {
				rowsEvents = append(rowsEvents, rowsEvent)
			}
			rowsEvent = isql.TableRowsEvent{}
			rowsEvent.Table = isql.Table{Name: string(t.Table), Schema: string(t.Schema)}
		case *replication.RowsQueryEvent:
			rowsEvent.Query = string(t.Query)
		case *replication.XIDEvent:
			rowsEvents = append(rowsEvents, rowsEvent)
			rowsEvent = isql.TableRowsEvent{}
			if len(src.Schemas) > 0 {
				rowsEventFiltered := rowsEvents[:0]

				for _, rowEv := range rowsEvents {
					for schemaGtidPos, schema := range src.Schemas {
						if rowEv.GetTable().GetSchema() != schema.Name {
							continue
						}

						//gtid of current schema more than all
						if len(schema.Gtid) > 0 && isGtidErlier(gtidSet, getGtidSet(schema.Gtid)) {
							continue
							//else we overtake schemas gtid
						} else if len(schema.Gtid) > 0 {
							src.Schemas[schemaGtidPos].Gtid = ""
						}

						//tables to sync
						if len(schema.TablesSync) > 0 {
							if contains(schema.TablesSync, rowEv.GetTable().GetName()) {
								rowsEventFiltered = append(rowsEventFiltered, rowEv)
							}
							// tables exclude
						} else if len(schema.TablesExclude) > 0 {
							if !contains(schema.TablesExclude, rowEv.GetTable().GetName()) {
								rowsEventFiltered = append(rowsEventFiltered, rowEv)
							}
						}
						//all tables in schema
						if len(schema.TablesSync) == 0 && len(schema.TablesExclude) == 0 {
							rowsEventFiltered = append(rowsEventFiltered, rowEv)
						}
					}
				}

				rowsEvents = rowsEventFiltered

				if len(rowsEvents) == 0 {
					continue
				}
			}

			send <- isql.RowsEvent{
				SourceName: src.Name,
				GtidSet:    gtidSetToString(gtidSet),
				TablesRows: rowsEvents,
			}

		case *replication.RotateEvent:
		case *replication.FormatDescriptionEvent:
		case *replication.GenericEvent:
		default:
			ev.Header.Dump(os.Stdout)
			ev.Dump(os.Stdout)
		}
		//context timeout
		cancel()
	}
}

func gtidSetToString(gs map[string]gtidInterval) (ret string) {
	for gtidUUID, gtidInterval := range gs {
		ret += gtidUUID + ":" + gtidInterval.Start + "-" + gtidInterval.Last + ","
	}
	ret = strings.Trim(ret, ",")

	return
}

var gtidSetReg = regexp.MustCompile(`([a-z0-9-]+):(\d+)([\d:-]*?)(\d*)$`)

type gtidInterval struct {
	Start string
	Last  string
}

func getGtidSet(gtidSet string) map[string]gtidInterval {
	var res = make(map[string]gtidInterval)

	gtids := strings.Split(gtidSet, ",")

	for _, gtidSet := range gtids {
		regresult := gtidSetReg.FindAllStringSubmatch(gtidSet, -1)
		res[regresult[0][1]] = gtidInterval{Start: regresult[0][2], Last: regresult[0][4]}
	}

	return res
}

func isGtidErlier(check, current map[string]gtidInterval) bool {
	for src, gt := range check {
		for csrc, cgt := range current {
			if src == csrc {
				f1, _ := strconv.Atoi(gt.Last)
				f2, _ := strconv.Atoi(cgt.Last)
				if f1 < f2 {
					return true
				}
			}
		}
	}

	return false
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func getVsqlFromDir(path string) (out string, err error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}

	var allVsql []string

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), `.sql`) {
			continue
		}

		dataBytes, err := ioutil.ReadFile(path + string(os.PathSeparator) + file.Name())
		if err != nil {
			return out, err
		}

		vsql, err := dumpAdopt(string(dataBytes))
		if err != nil {
			return out, err
		}

		allVsql = append(allVsql, vsql...)
	}

	for i, vsql := range allVsql {
		allVsql[i] = vsql + `;`
	}

	out = strings.Join(allVsql, "\n\n")

	return
}

func dumpAdopt(data string) (queries []string, err error) {
	var db = regexp.MustCompile("(?s)Database: ([[:print:]]+)")
	var comment = regexp.MustCompile("/\\*.*?\\*/")
	var commentLine = regexp.MustCompile("(?s)--.*?\n")

	reg := db.FindAllStringSubmatch(data, -1)

	var database string
	if len(reg) > 0 && len(reg[0]) > 1 {
		database = reg[0][1]
	}

	data = comment.ReplaceAllString(data, ``)
	data = commentLine.ReplaceAllString(data, ``)

	sqlsInFile := strings.Split(data, ";\n")

	var sqlsPurify []string
	for _, sql := range sqlsInFile {
		if sql := strings.TrimSpace(sql); len(sql) > 0 {
			sqlsPurify = append(sqlsPurify, sql)
		}
	}

	dest := new(vertica.Cache)
	for _, sql := range sqlsPurify {
		var vsql []string
		switch ddl := ddlparser.Ddlcase(sql, database).(type) {
		case isql.CreateTable:
			vsql = dest.GetTableSQL(ddl)
		case []isql.DropTable:
			vsql = dest.GetDropSQL(ddl)
		default:
			return queries, fmt.Errorf(`unknown ddl %s`, sql)
		}

		queries = append(queries, vsql...)
	}

	return queries, nil
}
