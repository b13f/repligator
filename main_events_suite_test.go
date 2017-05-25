package main

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/client"
	"github.com/stretchr/testify/suite"

	"github.com/b13f/repligator/isql"
)

type EventsTestSuite struct {
	suite.Suite
	c            *client.Conn
	gtid         string
	sourceName   string
	transactions []string
	cfg          configSource
}

func (s *EventsTestSuite) SetupSuite() {
	var err error

	s.c, err = client.Connect(fmt.Sprintf("%s:%d", *mysqlHost, 3306), "root", "", "")

	if err != nil {
		s.T().Log("can't connect to mysql")
		s.T().Fail()
	}
	_, _ = s.c.Execute("CREATE USER first")
	res, _ := s.c.Execute("SHOW MASTER STATUS")

	s.gtid, _ = res.GetStringByName(0, `Executed_Gtid_Set`)

	if len(s.gtid) == 0 {
		s.T().Log("empty gtidSet")
		s.T().Fail()
	}

	s.sourceName = `test`

	s.cfg = configSource{
		Name:     s.sourceName,
		Type:     `mysql`,
		ServerID: 100,
		Host:     *mysqlHost,
		Port:     3306,
		User:     `root`,
		Gtid:     s.gtid,
		Timeout:  1,
	}

	s.AddTx("CREATE DATABASE `testing1`")
	s.AddTx(`CREATE TABLE IF NOT EXISTS testing1.test (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		text VARCHAR(255) NOT NULL,
		datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	s.AddTx("INSERT INTO `testing1`.`test`(`text`) VALUES ('1111')")
	s.AddTx("INSERT INTO `testing1`.`test`(`text`) VALUES ('2222')")
	s.AddTx("INSERT INTO `testing1`.`test`(`text`) VALUES ('3333')")
	s.AddTx("INSERT INTO `testing1`.`test`(`text`) VALUES " + strings.Repeat(`('many'),`, 5200) + `('many')`)
	s.AddTx("UPDATE testing1.`test` SET text=\"4444\" WHERE text=\"3333\"")
	s.AddTx("DELETE FROM `testing1`.test WHERE text=\"2222\"")
	s.AddTx("INSERT INTO testing1.test(`text`) VALUES (\"5555\"),(\"6666\")")
	s.AddTx("UPDATE testing1.`test` SET datetime=\"2015-01-01 00:00:00\" LIMIT 2")

	s.c.Close()
	//default db
	s.c, _ = client.Connect(fmt.Sprintf("%s:%d", *mysqlHost, 3306), "root", "", "testing1")

	s.AddTx("CREATE TABLE test2 (    `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,    `text` VARCHAR(255) NOT NULL,    `datetime` DATETIME DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8")
	s.AddTx("INSERT INTO `testing1`.`test`(`text`) VALUES (\"9999\");INSERT INTO `test2`(`text`) VALUES (\"3333\");INSERT INTO `testing1`.`test2`(`id`,`text`) VALUES (\"333\",\"вапвап34пкупукпукп\")")
	s.AddTx(`CREATE TABLE IF NOT EXISTS testing1.test3 (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		cnt int(10),
		text VARCHAR(255) NOT NULL,
		datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
	    UNIQUE KEY (cnt)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	s.AddTx("INSERT INTO `testing1`.`test3`(`cnt`,`text`) VALUES (2,\"1111\"),(3,\"4444\"),(5,\"3333\"),(6,\"7777\")")
	s.AddTx("INSERT INTO test3(`cnt`,`text`) VALUES (2,\"11111\"),(3,\"44444\"),(6,\"7777\"),(99,\"111\"),(77,\"77\") ON DUPLICATE KEY UPDATE `text`='new'")
	s.AddTx("CREATE SCHEMA testing2")
	s.AddTx(`CREATE TABLE IF NOT EXISTS testing2.test3 (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	s.AddTx(`INSERT INTO testing2.test3 VALUES (1),(2)`)
	s.AddTx("INSERT INTO `test2`(`text`) VALUES (\"123123\")")
}

func (s *EventsTestSuite) TestCreateAndRowsEvents() {
	s.cfg.Schemas = []configSourceSchema{}
	sender := s.getSenderChan()

	var cmd string
	var sourceEvent interface{}

	i := 1
	nextGtid := func() string {
		i++
		return fmt.Sprintf("%s-%d", s.gtid, i)
	}

	sourceEvent = <-sender

	cmd = s.transactions[i-1]
	s.Assert().Equal(isql.DdlEvent{SourceName: s.sourceName, GtidSet: nextGtid(), Schema: `testing1`, Query: cmd}, sourceEvent)

	sourceEvent = <-sender

	cmd = s.transactions[i-1]
	s.Assert().Equal(isql.DdlEvent{SourceName: s.sourceName, GtidSet: nextGtid(), Schema: "", Query: cmd}, sourceEvent)

	sourceEvent = <-sender

	sourceEventT := sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(isql.Table{Schema: `testing1`, Name: `test`}, sourceEventT.GetTables()[0].GetTable())
	s.Assert().Equal(`insert`, sourceEventT.GetTables()[0].GetRows()[0].GetType())
	s.Assert().Equal(`1111`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])

	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	rowsCount := 0
	for ind := range sourceEventT.GetTables()[0].GetRows() {
		rowsCount += len(sourceEventT.GetTables()[0].GetRows()[ind].GetValues())
	}

	s.Assert().Equal(5201, rowsCount)

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`update`, sourceEventT.GetTables()[0].GetRows()[0].GetType())
	s.Assert().Equal(`3333`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
	s.Assert().Equal(`4444`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[1][1])

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`delete`, sourceEventT.GetTables()[0].GetRows()[0].GetType())
	s.Assert().Equal(`2222`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`5555`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
	s.Assert().Equal(`6666`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[1][1])

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`2015-01-01 00:00:00`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[1][2])
	s.Assert().Equal(`2015-01-01 00:00:00`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[3][2])

	<-sender
	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`test`, sourceEventT.GetTables()[0].GetTable().GetName())
	s.Assert().Equal(`test2`, sourceEventT.GetTables()[1].GetTable().GetName())
	s.Assert().Equal(`9999`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
	s.Assert().Equal(`вапвап34пкупукпукп`, sourceEventT.GetTables()[2].GetRows()[0].GetValues()[0][1])

	<-sender
	<-sender
	sourceEvent = <-sender

	sourceEventT = sourceEvent.(isql.RowsEvent)

	s.Equal(`testing1`, sourceEventT.GetTables()[0].GetTable().GetSchema())
	s.Equal(`update`, sourceEventT.GetTables()[0].GetRows()[0].GetType())
	s.Equal(`insert`, sourceEventT.GetTables()[0].GetRows()[1].GetType())
	s.Equal(`new`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[1][2])
	s.Equal(`new`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[5][2])
	s.Equal(int32(77), sourceEventT.GetTables()[0].GetRows()[1].GetValues()[1][1])
}

func (s *EventsTestSuite) TestConfigSchemaFilters() {
	s.cfg.Schemas = []configSourceSchema{{Name: `testing2`}}
	sender := s.getSenderChan()

	var cmd string
	var sourceEvent interface{}

	i := 1
	nextGtid := func() string {
		i++
		return fmt.Sprintf("%s-%d", s.gtid, i)
	}

	//ddls
	<-sender
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	i = len(s.transactions) - 3
	cmd = s.transactions[i-1]
	s.Assert().Equal(isql.DdlEvent{SourceName: s.sourceName, GtidSet: nextGtid(), Schema: `testing2`, Query: cmd}, sourceEvent)

	sourceEvent = <-sender

	cmd = s.transactions[i-1]
	s.Assert().Equal(isql.DdlEvent{SourceName: s.sourceName, GtidSet: nextGtid(), Schema: `testing1`, Query: cmd}, sourceEvent)
}

func (s *EventsTestSuite) TestConfigTablesSyncFilters() {
	s.cfg.Schemas = []configSourceSchema{{Name: `testing1`, TablesSync: []string{"test2"}}}
	sender := s.getSenderChan()

	var sourceEvent interface{}

	//ddls
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT := sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`test2`, sourceEventT.GetTables()[0].GetTable().GetName())
	s.Assert().Equal(`3333`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
	s.Assert().Equal(int64(333), sourceEventT.GetTables()[1].GetRows()[0].GetValues()[0][0])

	//ddls
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)
	s.Assert().Equal(`123123`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
}

func (s *EventsTestSuite) TestConfigTablesExcludeFilters() {
	s.cfg.Schemas = []configSourceSchema{{Name: `testing1`, TablesExclude: []string{"test", "test3"}}}
	sender := s.getSenderChan()

	var sourceEvent interface{}

	//ddls
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT := sourceEvent.(isql.RowsEvent)

	s.Assert().Equal(`test2`, sourceEventT.GetTables()[0].GetTable().GetName())
	s.Assert().Equal(`3333`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
	s.Assert().Equal(int64(333), sourceEventT.GetTables()[1].GetRows()[0].GetValues()[0][0])

	//ddls
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT = sourceEvent.(isql.RowsEvent)
	s.Assert().Equal(`123123`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
}

func (s *EventsTestSuite) TestConfigTablesGTIDFilters() {
	s.cfg.Schemas = []configSourceSchema{{Name: `testing1`, Gtid: s.gtid + `-17`}}
	sender := s.getSenderChan()

	var sourceEvent interface{}

	//ddls
	<-sender
	<-sender
	<-sender
	<-sender
	<-sender
	<-sender

	sourceEvent = <-sender
	sourceEventT := sourceEvent.(isql.RowsEvent)
	s.Assert().Equal(`123123`, sourceEventT.GetTables()[0].GetRows()[0].GetValues()[0][1])
}

func (s *EventsTestSuite) getSenderChan() <-chan interface{} {
	s.cfg.ServerID++

	sender := make(chan interface{})
	cancel := make(chan configSource)

	go listenSource(s.cfg, sender, cancel)

	return sender
}

func (s *EventsTestSuite) AddTx(sql string) {
	sqls := strings.Split(sql, ";")

	if len(sqls) > 1 {
		s.c.Begin()
	}

	for _, sqlEach := range sqls {
		_, err := s.c.Execute(sqlEach)
		if err != nil {
			s.T().Logf("\n%s\n", err.Error())
		}
	}

	if len(sqls) > 1 {
		s.c.Commit()
	}

	s.transactions = append(s.transactions, sql)
}
