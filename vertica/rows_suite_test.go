package vertica

import (
	"database/sql"
	"flag"
	"net/http/httptest"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/b13f/repligator/isql"
)

var verticaHost = flag.String("vertica", "127.0.0.1", "Vertica slave host")

type RowsTestSuite struct {
	suite.Suite
	v  *Cache
	ev []interface{}
}

func (s *RowsTestSuite) SetupSuite() {
	logrus.SetLevel(logrus.PanicLevel)

	verticaConf := Config{
		Odbc:       `Vertica`,
		Host:       *verticaHost,
		Port:       `5433`,
		User:       `dbadmin`,
		Password:   ``,
		Database:   `docker`,
		DataDir:    `/tmp`,
		FlushCount: 1,
	}

	var err error

	if s.v, err = Init(verticaConf); err != nil {
		s.FailNow(err.Error())
	}

	//suite.AddTx("CREATE SCHEMA `testing23`/*some comment*/ DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci")
	//suite.AddTx(`CREATE TABLE testing23.all_mysql_types (
	//  my_bit bit(1) DEFAULT NULL,
	//  my_tinyint tinyint(4) DEFAULT NULL,
	//  my_boolean tinyint(1) DEFAULT NULL,
	//  my_smallint smallint(6) DEFAULT NULL,
	//  my_mediumint mediumint(9) DEFAULT NULL,
	//  my_int int(11) DEFAULT NULL,
	//  my_bigint bigint(20) DEFAULT NULL,
	//  my_decimal_10_5 decimal(10,5) DEFAULT NULL,
	//  my_udecimal_10_5 decimal(10,5) unsigned DEFAULT NULL,
	//  my_float float DEFAULT NULL,
	//  my_ufloat float unsigned DEFAULT NULL,
	//  my_double double DEFAULT NULL,
	//  my_udouble double unsigned DEFAULT NULL,
	//  my_date date DEFAULT NULL,
	//  my_datetime datetime DEFAULT NULL,
	//  my_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	//  my_time time DEFAULT NULL,
	//  my_year year(4) DEFAULT NULL,
	//  my_char_10 char(10) DEFAULT NULL,
	//  my_varchar_10 varchar(10) DEFAULT NULL,
	//  my_binary binary(100) DEFAULT NULL,
	//  my_varbinary varbinary(100) DEFAULT NULL,
	//  my_tinyblob tinyblob,
	//  my_tinytext tinytext,
	//  my_blob blob,
	//  my_text text,
	//  my_mediumblob mediumblob,
	//  my_mediumtext mediumtext,
	//  my_longblob longblob,
	//  my_longtext longtext,
	//  my_enum_abc enum('a','b','c') DEFAULT NULL,
	//  my_enum_string enum('alpha','beta','gamma') DEFAULT NULL,
	//  my_set_def set("d","e","f") DEFAULT NULL
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_bit) VALUES(1)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_tinyint) VALUES(21)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_boolean) VALUES(1)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_smallint) VALUES(123)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_mediumint) VALUES(123456)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_int) VALUES(-1234567)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_bigint) VALUES(1234567890)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_decimal_10_5) VALUES(-12345.1234)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_udecimal_10_5) VALUES(12345.1234)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_float) VALUES(-32412.3452345)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_ufloat) VALUES(2342.3453253)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_double) VALUES(123456.1234560)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_udouble) VALUES(12345.123456)")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_date) VALUES('2017-01-23')")
	//
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_datetime) VALUES('2017-01-23 00:11:12')")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_timestamp) VALUES('2017-01-23 00:11:12')")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_time) VALUES('00:11:12')")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_year) VALUES('2017')")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_char_10) VALUES(\"qwertasdfg\")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_varchar_10) VALUES(\"qwerty\")")
	//suite.AddTx(`CREATE TABLE IF NOT EXISTS testing23.skip (
	//	id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	//	cnt int(10),
	//	PRIMARY KEY (id)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_tinytext) VALUES('sadfasdf')")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_enum_abc) VALUES(\"b\")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_enum_string) VALUES(\"gamma\")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_text) VALUES(\"asdf43f34f34f34f43f43f34f43f43f43f43f\")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_mediumtext) VALUES(\" cbvbccvcbc   g dg df df df dfg df \")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_tinytext) VALUES(\"asdf4879798ol789ol789lo789luykutuuf\")")
	//suite.AddTx("INSERT INTO testing23.all_mysql_types(my_longtext) VALUES(\"asdf4879798ol789ol789lo789luykutuuf 2g2g25g45g25g424524g54gg\")")
	//
	//suite.AddTx("DELETE FROM testing23.all_mysql_types WHERE `my_bit`=1")
	//
	//suite.AddTx(`CREATE TABLE IF NOT EXISTS testing23.test3 (
	//	id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	//	cnt int(10),
	//	text VARCHAR(255) NOT NULL,
	//	datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
	//	PRIMARY KEY (id),
	//    UNIQUE KEY (cnt)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	//suite.AddTx("INSERT INTO `testing23`.`test3`(`cnt`,`text`) VALUES (2,\"1111\"),(3,\"4444\"),(5,\"3333\"),(6,\"7777\")")
	//suite.AddTx("DELETE FROM `testing23`.`test3` LIMIT 2")
	//suite.AddTx("UPDATE testing23.all_mysql_types SET `my_datetime`='2017-01-23 00:11:12' WHERE `my_tinyint`=21")
	//
	//suite.AddTx(`CREATE TABLE IF NOT EXISTS testing23.test4 (
	//	id bigint(20) unsigned NOT NULL,
	//	cnt int(10),
	//	text VARCHAR(255) NOT NULL,
	//    UNIQUE KEY (id,cnt)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8`)
	//suite.AddTx("INSERT INTO `testing23`.`test4`(`id`,`cnt`,`text`) VALUES (1,2,\"1111\"),(2,3,\"4444\"),(3,4,\"3333\"),(4,5,\"7777\")")
	//suite.AddTx("UPDATE `testing23`.`test4` SET `text`='!!!!' WHERE id>0")

	//generated events from mysql
	s.ev = []interface{}{
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-21", Schema: "testing23", Query: "CREATE SCHEMA `testing23`/*some comment*/ DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci"},
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-22", Schema: "", Query: "CREATE TABLE testing23.all_mysql_types (\n\t  my_bit bit(1) DEFAULT NULL,\n\t  my_tinyint tinyint(4) DEFAULT NULL,\n\t  my_boolean tinyint(1) DEFAULT NULL,\n\t  my_smallint smallint(6) DEFAULT NULL,\n\t  my_mediumint mediumint(9) DEFAULT NULL,\n\t  my_int int(11) DEFAULT NULL,\n\t  my_bigint bigint(20) DEFAULT NULL,\n\t  my_decimal_10_5 decimal(10,5) DEFAULT NULL,\n\t  my_udecimal_10_5 decimal(10,5) unsigned DEFAULT NULL,\n\t  my_float float DEFAULT NULL,\n\t  my_ufloat float unsigned DEFAULT NULL,\n\t  my_double double DEFAULT NULL,\n\t  my_udouble double unsigned DEFAULT NULL,\n\t  my_date date DEFAULT NULL,\n\t  my_datetime datetime DEFAULT NULL,\n\t  my_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n\t  my_time time DEFAULT NULL,\n\t  my_year year(4) DEFAULT NULL,\n\t  my_char_10 char(10) DEFAULT NULL,\n\t  my_varchar_10 varchar(10) DEFAULT NULL,\n\t  my_binary binary(100) DEFAULT NULL,\n\t  my_varbinary varbinary(100) DEFAULT NULL,\n\t  my_tinyblob tinyblob,\n\t  my_tinytext tinytext,\n\t  my_blob blob,\n\t  my_text text,\n\t  my_mediumblob mediumblob,\n\t  my_mediumtext mediumtext,\n\t  my_longblob longblob,\n\t  my_longtext longtext,\n\t  my_enum_abc enum('a','b','c') DEFAULT NULL,\n\t  my_enum_string enum('alpha','beta','gamma') DEFAULT NULL,\n\t  my_set_def set(\"d\",\"e\",\"f\") DEFAULT NULL\n\t) ENGINE=InnoDB DEFAULT CHARSET=utf8"},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-23", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{1, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-24", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), 21, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-25", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), 1, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-26", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), 123, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-27", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 123456, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-28", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), -1234567, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-29", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 1234567890, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-30", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), -12345.1234, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-31", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 12345.1234, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-32", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), -32412.346, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-33", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 2342.3452, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-34", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 123456.123456, interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-35", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), 12345.123456, interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-36", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-01-23", interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-37", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-01-23 00:11:12", "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-38", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-01-23 03:11:12", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-39", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", "00:11:12", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-40", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), 2017, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-41", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), "qwertasdfg", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-42", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), "qwerty", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-43", Schema: "", Query: "CREATE TABLE IF NOT EXISTS testing23.skip (\n\t\tid bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n\t\tcnt int(10),\n\t\tPRIMARY KEY (id)\n\t) ENGINE=InnoDB DEFAULT CHARSET=utf8"},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-44", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), []uint8{0x73, 0x61, 0x64, 0x66, 0x61, 0x73, 0x64, 0x66}, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-45", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), int64(2), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-46", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), int64(3), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-47", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), []uint8{0x61, 0x73, 0x64, 0x66, 0x34, 0x33, 0x66, 0x33, 0x34, 0x66, 0x33, 0x34, 0x66, 0x33, 0x34, 0x66, 0x34, 0x33, 0x66, 0x34, 0x33, 0x66, 0x33, 0x34, 0x66, 0x34, 0x33, 0x66, 0x34, 0x33, 0x66, 0x34, 0x33, 0x66, 0x34, 0x33, 0x66}, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-48", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), []uint8{0x20, 0x63, 0x62, 0x76, 0x62, 0x63, 0x63, 0x76, 0x63, 0x62, 0x63, 0x20, 0x20, 0x20, 0x67, 0x20, 0x64, 0x67, 0x20, 0x64, 0x66, 0x20, 0x64, 0x66, 0x20, 0x64, 0x66, 0x20, 0x64, 0x66, 0x67, 0x20, 0x64, 0x66, 0x20}, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-49", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), []uint8{0x61, 0x73, 0x64, 0x66, 0x34, 0x38, 0x37, 0x39, 0x37, 0x39, 0x38, 0x6f, 0x6c, 0x37, 0x38, 0x39, 0x6f, 0x6c, 0x37, 0x38, 0x39, 0x6c, 0x6f, 0x37, 0x38, 0x39, 0x6c, 0x75, 0x79, 0x6b, 0x75, 0x74, 0x75, 0x75, 0x66}, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-50", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), []uint8{0x61, 0x73, 0x64, 0x66, 0x34, 0x38, 0x37, 0x39, 0x37, 0x39, 0x38, 0x6f, 0x6c, 0x37, 0x38, 0x39, 0x6f, 0x6c, 0x37, 0x38, 0x39, 0x6c, 0x6f, 0x37, 0x38, 0x39, 0x6c, 0x75, 0x79, 0x6b, 0x75, 0x74, 0x75, 0x75, 0x66, 0x20, 0x32, 0x67, 0x32, 0x67, 0x32, 0x35, 0x67, 0x34, 0x35, 0x67, 0x32, 0x35, 0x67, 0x34, 0x32, 0x34, 0x35, 0x32, 0x34, 0x67, 0x35, 0x34, 0x67, 0x67}, interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-51", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "delete", Values: [][]interface{}{[]interface{}{1, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-52", Schema: "", Query: "CREATE TABLE IF NOT EXISTS testing23.test3 (\n\t\tid bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n\t\tcnt int(10),\n\t\ttext VARCHAR(255) NOT NULL,\n\t\tdatetime DATETIME DEFAULT CURRENT_TIMESTAMP,\n\t\tPRIMARY KEY (id),\n\t    UNIQUE KEY (cnt)\n\t) ENGINE=InnoDB DEFAULT CHARSET=utf8"},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-53", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "test3"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{1, 2, "1111", "2017-05-04 11:14:47"}, []interface{}{2, 3, "4444", "2017-05-04 11:14:47"}, []interface{}{3, 5, "3333", "2017-05-04 11:14:47"}, []interface{}{4, 6, "7777", "2017-05-04 11:14:47"}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-54", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "test3"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "delete", Values: [][]interface{}{[]interface{}{1, 2, "1111", "2017-05-04 11:14:47"}, []interface{}{2, 3, "4444", "2017-05-04 11:14:47"}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-55", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "all_mysql_types"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "update", Values: [][]interface{}{[]interface{}{interface{}(nil), 21, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-05-04 14:14:46", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}, []interface{}{interface{}(nil), 21, interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), "2017-01-23 00:11:12", "2017-05-04 14:14:47", interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil), interface{}(nil)}}}}}}},
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-56", Schema: "", Query: "CREATE TABLE IF NOT EXISTS testing23.test4 (\n\t\tid bigint(20) unsigned NOT NULL,\n\t\tcnt int(10),\n\t\ttext VARCHAR(255) NOT NULL,\n\t    UNIQUE KEY (id,cnt)\n\t) ENGINE=InnoDB DEFAULT CHARSET=utf8"},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-57", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "test4"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{1, 2, "1111"}, []interface{}{2, 3, "4444"}, []interface{}{3, 4, "3333"}, []interface{}{4, 5, "7777"}}}}}}},
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-58", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "test4"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "update", Values: [][]interface{}{[]interface{}{1, 2, "1111"}, []interface{}{1, 2, "!!!!"}, []interface{}{2, 3, "4444"}, []interface{}{2, 3, "!!!!"}, []interface{}{3, 4, "3333"}, []interface{}{3, 4, "!!!!"}, []interface{}{4, 5, "7777"}, []interface{}{4, 5, "!!!!"}}}}}}},
	}

	skip := make(chan string)
	sender := make(chan interface{})

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev[:9] {
		sender <- ev
	}
	sender <- true

	if s.v.isCacheExist() {
		s.v.clearCache()
	}

	pos, _ := s.v.GetLastPosition(`test`)
	s.Equal("97570b38-30b9-11e7-a0a1-0242ac110001:1-29", pos)

	s.v.flushCount = 6
	s.v.flushTime = 2

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev[9:31] {
		sender <- ev
	}
	sender <- true
	s.v.clearCache()

	pos, _ = s.v.GetLastPosition(`test`)
	s.Equal("97570b38-30b9-11e7-a0a1-0242ac110001:1-51", pos)

	s.v.flushCount = 1

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev[31:] {
		sender <- ev
	}
	sender <- true
	s.v.clearCache()
}

func (s *RowsTestSuite) TestRows() {
	var db *sql.DB
	var err error

	if db, err = sql.Open("odbc", s.v.ODBCdsn); err != nil {
		s.FailNow(err.Error())
	}

	rows, err := db.Query(`SELECT my_enum_string,my_longtext FROM testing23.all_mysql_types ORDER BY my_enum_string,my_longtext`)
	if err != nil {
		s.FailNow(err.Error())
	}

	var enum, text sql.NullString
	var all [][]sql.NullString

	for rows.Next() {
		if err = rows.Scan(&enum, &text); err != nil {
			s.FailNow(err.Error())
		}

		all = append(all, []sql.NullString{enum, text})
	}

	rows.Close()

	s.Equal(26, len(all))
	s.Equal(`gamma`, all[0][0].String)
	s.Equal("asdf4879798ol789ol789lo789luykutuuf 2g2g25g45g25g424524g54gg", all[1][1].String)
}

func (s *RowsTestSuite) TestFailRows() {
	s.ev = []interface{}{
		isql.RowsEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac112222:1-132", TablesRows: []isql.TableRowsEvent{isql.TableRowsEvent{Table: isql.Table{Schema: "testing23", Name: "test4"}, Query: "", Rows: []isql.Rows{isql.Rows{Type: "insert", Values: [][]interface{}{[]interface{}{1, 2, "1111"}, []interface{}{2, 3, "4444"}, []interface{}{3, 4, "3333"}, []interface{}{4, 5, "7777"}}}}}}},
	}

	skip := make(chan string)
	sender := make(chan interface{})

	err := s.v.ApplyEvent(sender, skip)

	for _, ev := range s.ev {
		sender <- ev
	}
	error := <-err
	sender <- true

	s.Equal(true, strings.Contains(error.Error(), `Duplicate key values`))

	//clear failed cache rows
	s.v.tables = make(map[string]tableCache)
}

func (s *RowsTestSuite) TestSkipTransaction() {
	s.ev = []interface{}{
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-59", Schema: "", Query: "ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL"},
	}

	skip := make(chan string)
	sender := make(chan interface{})

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev {
		sender <- ev
	}

	s.Equal("ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL", <-skip)
	sender <- true
}

func (s *RowsTestSuite) TestBotInterfaces() {
	s.ev = []interface{}{
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-59", Schema: "", Query: "ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL"},
	}

	skip := make(chan string)
	sender := make(chan interface{})

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev {
		sender <- ev
	}

	botCmds := s.v.GetBotInterfaces(skip)

	s.Equal(`vsql done`, botCmds[`vsql`](`vsql CREATE SCHEMA IF NOT EXISTS "test_bot"`))
	s.Equal("Transaction: ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL skipped",
		botCmds[`skip`](`skip`))

	sender <- true
}

func (s *RowsTestSuite) TestHttpInterfaces() {
	s.ev = []interface{}{
		isql.DdlEvent{SourceName: "test", GtidSet: "97570b38-30b9-11e7-a0a1-0242ac110001:1-60", Schema: "", Query: "ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL"},
	}

	skip := make(chan string)
	sender := make(chan interface{})

	_ = s.v.ApplyEvent(sender, skip)
	for _, ev := range s.ev {
		sender <- ev
	}

	handlers := s.v.GetHTTPInterfaces(skip)

	w := httptest.NewRecorder()
	handlers[`/skip`](w, httptest.NewRequest(`GET`, `/skip`, nil))
	w.Result()

	s.Equal("Transaction: ALTER TABLE testing23.test3 MODIFY COLUMN `datetime`DATETIME DEFAULT NULL skipped",
		w.Body.String())

	sender <- true
}
