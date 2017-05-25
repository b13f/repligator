package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

//docker run -d -p 3306:3306 --name mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes percona/percona-server:latest --binlog_format=ROW --binlog_row_image=full --server-id=1 --log-bin=/tmp/bin.log --gtid-mode=ON --enforce-gtid-consistency
//docker run -d -p 5433:5433 -m 1g --name vertica jbfavre/vertica:latest
//db:docker user:dbadmin

var mysqlHost = flag.String("mysql", "127.0.0.1", "MySQL master host")

func TestEventsSuite(t *testing.T) {
	suite.Run(t, new(EventsTestSuite))
}

func TestGetGtidSetCase(t *testing.T) {
	gtids := []string{"a97faa30-1db7-11e6-b644-c81f66bb686c:1",
		"a97faa30-1db7-11e6-b644-c81f66bb686c:1-787249579:787249581-868158559,f696c524-1e99-11e6-ac50-c81f66bb6200:1-59",
		"1cb543e8-28c6-11e6-a154-b083fec29b80:1-26,a920e3ce-1e7f-11e6-8af3-c81f66bb60d8:1-312,a97faa30-1db7-11e6-b644-c81f66bb686c:105681229-563018028,b9552b52-28dc-11e6-823a-b083fec2a722:1-354345315,f696c524-1e99-11e6-ac50-c81f66bb6200:41-49348786"}

	for _, gtid := range gtids {
		gtidHash := getGtidSet(gtid)
		t.Logf("\n%v\n", gtidHash)
	}
}
