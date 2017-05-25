package isql

//Table name struct
type Table struct {
	Schema string
	Name   string
}

//GetSchema return schema
func (t Table) GetSchema() string {
	return t.Schema
}

//GetName return table name
func (t Table) GetName() string {
	return t.Name
}

//Column is MySql column description
type Column struct {
	Name string
	Type string
}

//GetName return name
func (c Column) GetName() string {
	return c.Name
}

//GetType return MySQL type
func (c Column) GetType() string {
	return c.Type
}

//Constraint is constraint description struct
type Constraint struct {
	Name    string
	Type    string
	Columns []string
}

//GetName return name
func (k Constraint) GetName() string {
	return k.Name
}

//GetType return type
func (k Constraint) GetType() string {
	return k.Type
}

//GetColumns return column names
func (k Constraint) GetColumns() []string {
	return k.Columns
}

//RenameTable description of RENAME DDL
type RenameTable struct {
	From Table
	To   Table
}

//GetFrom return Table rename from
func (r RenameTable) GetFrom() Table {
	return r.From
}

//GetTo return Table rename to
func (r RenameTable) GetTo() Table {
	return r.To
}

//TruncateTable description of TRUNCATE DDL
type TruncateTable struct {
	Table
}

//CreateSchema description of CREATE SCHEMA DDL
type CreateSchema struct {
	Name string
}

//GetName return name
func (c CreateSchema) GetName() string {
	return c.Name
}

//CreateTable description of CREATE TABLE DDL
type CreateTable struct {
	Table       Table
	Columns     []Column
	Constraints []Constraint
}

//GetCreateTable return Table to create
func (c CreateTable) GetCreateTable() Table {
	return c.Table
}

//GetColumns return Columns
func (c CreateTable) GetColumns() []Column {
	return c.Columns
}

// GetConstraints return Constraints
func (c CreateTable) GetConstraints() []Constraint {
	return c.Constraints
}

//CreateTableLike description of CREATE TABLE LIKE DDL
type CreateTableLike struct {
	Table     Table
	LikeTable Table
}

//GetTable return Table to create
func (c CreateTableLike) GetTable() Table {
	return c.Table
}

//GetLikeTable return Table like
func (c CreateTableLike) GetLikeTable() Table {
	return c.LikeTable
}

//DropTable description of DROP TABLE DDL
type DropTable struct {
	Table
}

//AlterTable description of ALTER TABLE DDL
type AlterTable struct {
	Table          Table
	AddColumns     []Column
	DropColumns    []Column
	AddConstraints []Constraint
}

//GetAlterTable return Table to alter
func (a AlterTable) GetAlterTable() Table {
	return a.Table
}

//GetAddColumns return columns to add
func (a AlterTable) GetAddColumns() []Column {
	return a.AddColumns
}

//GetDropColumns return column to drop
func (a AlterTable) GetDropColumns() []Column {
	return a.DropColumns
}

//GetAddConstraints return constraints to add
func (a AlterTable) GetAddConstraints() []Constraint {
	return a.AddConstraints
}

//Rows description of rows with values
type Rows struct {
	Type   string
	Values [][]interface{}
}

//GetType return rows event type
func (r Rows) GetType() string {
	return r.Type
}

//GetValues return rows values
func (r Rows) GetValues() [][]interface{} {
	return r.Values
}

//TableRowsEvent description of rows events on table
type TableRowsEvent struct {
	Table Table
	Query string
	Rows  []Rows
}

//GetTable return Table
func (tre TableRowsEvent) GetTable() Table {
	return tre.Table
}

//GetRows return Rows
func (tre TableRowsEvent) GetRows() []Rows {
	return tre.Rows
}

//RowsEvent description of rows transaction
type RowsEvent struct {
	SourceName string
	GtidSet    string
	TablesRows []TableRowsEvent
}

//GetSourceName return source
func (re RowsEvent) GetSourceName() string {
	return re.SourceName
}

//GetGtidSet return gtid set of current transaction
func (re RowsEvent) GetGtidSet() string {
	return re.GtidSet
}

//GetTables return rows events on tables
func (re RowsEvent) GetTables() []TableRowsEvent {
	return re.TablesRows
}

//DdlEvent description of ddl transaction
type DdlEvent struct {
	SourceName string
	GtidSet    string
	Schema     string
	Query      string
}

//GetSourceName return source
func (de DdlEvent) GetSourceName() string {
	return de.SourceName
}

//GetGtidSet return gtid set of current transaction
func (de DdlEvent) GetGtidSet() string {
	return de.GtidSet
}

//GetSchema return schema of current event
func (de DdlEvent) GetSchema() string {
	return de.Schema
}

//GetQuery return MySQL DDL query
func (de DdlEvent) GetQuery() string {
	return de.Query
}
