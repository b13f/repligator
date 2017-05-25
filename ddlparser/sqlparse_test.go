package ddlparser

import (
	"testing"

	"github.com/b13f/repligator/isql"
	"github.com/stretchr/testify/suite"
)

type DDLParseTestSuite struct {
	suite.Suite
}

func (s *DDLParseTestSuite) TestCreateTable() {
	sql := `CREATE TABLE employees (
			emp_no      INT             NOT NULL,
			birth_date  DATE            NOT NULL,
			first_name  VARCHAR(14)     NOT NULL,
			last_name   VARCHAR(16)     NOT NULL,
			gender      ENUM ('M','F')  NOT NULL,
			hire_date   DATE            NOT NULL,
			PRIMARY KEY (emp_no)
		)`

	dp, _ := newDdlParser(sql, ``)

	s.Equal(`employees`, dp.getTypeStruct().(isql.CreateTable).GetCreateTable().GetName())
	s.Equal(6, len(dp.getTypeStruct().(isql.CreateTable).GetColumns()))
	s.Equal(`ENUM('M','F')`, dp.getTypeStruct().(isql.CreateTable).GetColumns()[4].GetType())

	sql = `CREATE TABLE departments (
				dept_no     CHAR(4)         NOT NULL,
				dept_name   VARCHAR(40)     NOT NULL,
				PRIMARY KEY (dept_no),
				UNIQUE  KEY (dept_name)
			)`

	dp, _ = newDdlParser(sql, ``)

	s.Equal(`departments`, dp.getTypeStruct().(isql.CreateTable).GetCreateTable().GetName())
	s.Equal(2, len(dp.getTypeStruct().(isql.CreateTable).GetColumns()))
	s.Equal(primary, dp.getTypeStruct().(isql.CreateTable).GetConstraints()[0].GetType())

	sql = `CREATE TABLE dept_emp (
				emp_no      INT             NOT NULL,
				dept_no     CHAR(4)         NOT NULL,
				from_date   DATE            NOT NULL,
				to_date     DATE            NOT NULL,
				FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no)  ON DELETE CASCADE,
				FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
				PRIMARY KEY (emp_no,dept_no)
			)`

	dp, _ = newDdlParser(sql, ``)

	s.Equal(4, len(dp.getTypeStruct().(isql.CreateTable).GetColumns()))
	s.Equal(primary, dp.getTypeStruct().(isql.CreateTable).GetConstraints()[0].GetType())

	sql = `CREATE TABLE test.dept_emp (
				emp_no      INT             NOT NULL comment '&$!@#@!#@!#@%^&**(VSDAQ""',
				dept_no     CHAR(4)         NOT NULL,
				from_date   DATE            NOT NULL,
				to_date     DATE            NOT NULL,
				PRIMARY KEY (emp_no,dept_no)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	dp, _ = newDdlParser(sql, ``)

	s.Equal(`test`, dp.getTypeStruct().(isql.CreateTable).GetCreateTable().GetSchema())
	s.Equal(4, len(dp.getTypeStruct().(isql.CreateTable).GetColumns()))

	sql = "CREATE TABLE test1.`dep` (dept_no CHAR(4) NOT NULL, `dept_name` VARCHAR(40) NOT NULL,PRIMARY KEY (dept_no))"

	dp, _ = newDdlParser(sql, ``)

	s.Equal(`dep`, dp.getTypeStruct().(isql.CreateTable).GetCreateTable().GetName())
	s.Equal(`dept_name`, dp.getTypeStruct().(isql.CreateTable).GetColumns()[1].GetName())

	sql = "CREATE TABLE test1.`dep` LIKE test2.`dep2`"

	dp, _ = newDdlParser(sql, ``)

	s.Equal(`dep`, dp.getTypeStruct().(isql.CreateTableLike).GetTable().GetName())
	s.Equal(`test2`, dp.getTypeStruct().(isql.CreateTableLike).GetLikeTable().GetSchema())
}

func (s *DDLParseTestSuite) TestSupportedAlterTable() {
	sql := "ALTER TABLE `test`.`users_test` ADD COLUMN `type` VARCHAR(255) DEFAULT NULL"

	dp, _ := newDdlParser(sql, ``)

	s.Equal(`users_test`, dp.getTypeStruct().(isql.AlterTable).GetAlterTable().GetName())
	s.Equal(`test`, dp.getTypeStruct().(isql.AlterTable).GetAlterTable().GetSchema())
	s.Equal(0, len(dp.getTypeStruct().(isql.AlterTable).GetDropColumns()))
	s.Equal(`VARCHAR(255)`, dp.getTypeStruct().(isql.AlterTable).GetAddColumns()[0].GetType())

	sql = "ALTER TABLE `test`.`day_statistic_monthly` ADD KEY `user_id`(`user_id`)"

	dp, _ = newDdlParser(sql, ``)

	s.Equal(nil, dp.getTypeStruct())

	sql = "ALTER TABLE test.`traffic` DROP INDEX key_uniq"

	dp, _ = newDdlParser(sql, ``)

	s.Equal(nil, dp.getTypeStruct())

	sql = "ALTER TABLE `traffic` ADD UNIQUE KEY `key_uniq` (`date`,`resource`,`country`,`city`,`utm_campaign`)"

	dp, _ = newDdlParser(sql, `test`)

	s.Equal(`test`, dp.getTypeStruct().(isql.AlterTable).GetAlterTable().GetSchema())
	s.Equal(`resource`, dp.getTypeStruct().(isql.AlterTable).GetAddConstraints()[0].GetColumns()[1])

	sql = `ALTER TABLE auto
			ADD COLUMN status TINYINT(1) NOT NULL DEFAULT 0,
			ADD COLUMN reason VARCHAR(255) NULL DEFAULT NULL`

	dp, _ = newDdlParser(sql, ``)

	s.Equal(0, len(dp.getTypeStruct().(isql.AlterTable).GetAddConstraints()))
	s.Equal(2, len(dp.getTypeStruct().(isql.AlterTable).GetAddColumns()))
	s.Equal(`VARCHAR(255)`, dp.getTypeStruct().(isql.AlterTable).GetAddColumns()[1].GetType())

	sql = `ALTER TABLE test.traffic ADD count_intersection INT UNSIGNED NOT NULL`

	dp, _ = newDdlParser(sql, ``)

	s.Equal(`count_intersection`, dp.getTypeStruct().(isql.AlterTable).GetAddColumns()[0].GetName())

	sql = "ALTER TABLE `test`.`dept_emp` DROP COLUMN `result`;"

	dp, _ = newDdlParser(sql, `test2`)

	s.Equal(`test`, dp.getTypeStruct().(isql.AlterTable).GetAlterTable().GetSchema())
	s.Equal(`result`, dp.getTypeStruct().(isql.AlterTable).GetDropColumns()[0].GetName())

	sql = "ALTER TABLE dept_emp ADD UNIQUE INDEX `user_type_UNIQUE` (`user_id`, `type`)"

	dp, _ = newDdlParser(sql, `test2`)

	s.Equal(`test2`, dp.getTypeStruct().(isql.AlterTable).GetAlterTable().GetSchema())
	s.Equal(`user_id`, dp.getTypeStruct().(isql.AlterTable).GetAddConstraints()[0].GetColumns()[0])
}

func (s *DDLParseTestSuite) TestUnsupportedAlterTable() {
	sqls := []string{
		"ALTER TABLE `dept_emp` ADD COLUMN `count` int(11) NOT NULL DEFAULT '0' AFTER `date`",
		"ALTER TABLE `test`.`dept_emp` CHANGE COLUMN `value` `value` TEXT NOT NULL COMMENT ''",
		"ALTER TABLE `day_statistic` MODIFY `users` int(10) unsigned NOT NULL DEFAULT 0",
		"ALTER TABLE `dept_emp` MODIFY COLUMN `users` DATE NOT NULL",
	}

	for _, sql := range sqls {
		dp, _ := newDdlParser(sql, ``)
		s.Equal(`Non supported ALTER`, dp.getTypeStruct().(error).Error())
	}
}

func (s *DDLParseTestSuite) TestCreateSchema() {
	sql := "CREATE DATABASE test"

	dp, _ := newDdlParser(sql, `test2`)

	s.Equal(`test`, dp.getTypeStruct().(isql.CreateSchema).GetName())
}

func (s *DDLParseTestSuite) TestTruncateTable() {
	sql := "TRUNCATE test1.test"

	dp, _ := newDdlParser(sql, `test2`)

	s.Equal(`test1`, dp.getTypeStruct().(isql.TruncateTable).GetSchema())
}

func (s *DDLParseTestSuite) TestRenameTable() {
	sql := "RENAME TABLE test1.test TO test2, test4 TO test1.test2"

	dp, _ := newDdlParser(sql, `test3`)

	s.Equal(`test`, dp.getTypeStruct().([]isql.RenameTable)[0].GetFrom().GetName())
	s.Equal(`test3`, dp.getTypeStruct().([]isql.RenameTable)[0].GetTo().GetSchema())
	s.Equal(`test3`, dp.getTypeStruct().([]isql.RenameTable)[1].GetFrom().GetSchema())
	s.Equal(`test2`, dp.getTypeStruct().([]isql.RenameTable)[1].GetTo().GetName())
}

func (s *DDLParseTestSuite) TestDropTable() {
	sql := "DROP TABLE test1.test,test2"

	dp, _ := newDdlParser(sql, `test2`)

	s.Equal(`test`, dp.getTypeStruct().([]isql.DropTable)[0].GetName())
	s.Equal(`test2`, dp.getTypeStruct().([]isql.DropTable)[1].GetSchema())
}

func (s *DDLParseTestSuite) TestSkippedDDL() {
	sqls := []string{
		"DROP TEMPORARY TABLE IF EXISTS `test`.`dept_emp` /* generated by server */",
	}

	for _, sql := range sqls {
		r := Ddlcase(sql, ``)
		s.Equal(nil, r)
	}
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(DDLParseTestSuite))
}
