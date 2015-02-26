package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
	"os"
	"strings"
)

var debug = flag.Bool("debug", false, "enable debugging")
var server = flag.String("server", "fecsql03", "the database server")
var database = flag.String("database", "Internal", "the database ")
var table = flag.String("table", "", "the database dataTable")
var user = flag.String("user", "SPWebProg", "the database user")
var password = flag.String("password", "", "the user password")
var port = flag.Int("port", 1433, "the database port")

type DataTable struct {
	name    string
	columns []Column
}

type Column struct {
	dataTable_name string
	column_name    string
	data_type      string
	max_length     int
	precision      int
	column_id      int
	is_identity    bool
	is_computed    bool
}

func main() {
	flag.Parse() // parse the command line args
	processDataTable("EmployeeIT")
}

// getConnectionString returns connection string for the SqlServer
func getConnectionString() string {
	connString := fmt.Sprintf("server=%s;port=%d;database=%s;user=%s;password=%s", *server, *port, *database, *user, *password)
	return connString
}

// Given a column, return the SQL Parameter information
// i.e. @hourlyWage decimal(10,3)
func getMetaData(column Column) string {
	// char, varchar, nvarchar, nchar, decimal, float, numeric
	var buffer bytes.Buffer

	size := ""

	switch column.data_type {
	case "char", "varchar", "nvarchar":
		size = fmt.Sprintf("(%d)", column.max_length)
	case "decimal", "float", "numeric":
		size = fmt.Sprintf("(%d, %d)", column.max_length, column.precision)
	}

	buffer.WriteString(fmt.Sprintf("@%s %s%s ", column.column_name, column.data_type, size))

	return buffer.String()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// processDataTable calls the functions that generate the code
func processDataTable(dataTableName string) {
	dataTable := loadDataTable(dataTableName)

	sprocs := makeSqlCode(dataTable)
	class := makeClassCode(dataTable)
	sprocFile, err := os.Create(fmt.Sprintf("CREATE_%s.sql", dataTableName))
	check(err)

	defer sprocFile.Close()

	_, err = sprocFile.WriteString(sprocs)
	sprocFile.Sync()

	classFile, err := os.Create(fmt.Sprintf("C:\\client\\Current\\Common\\Internal\\Internal\\%s.cs", dataTableName))
	check(err)
	defer classFile.Close()

	_, err = classFile.WriteString(class)
	classFile.Sync()

}

// loadDataTable grabs the dataTable and column details from the database
func loadDataTable(dataTableName string) DataTable {
	dataTable := DataTable{}
	dataTable.name = dataTableName

	connString := getConnectionString()
	conn, err := sql.Open("mssql", connString)

	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	sql := `select a.name as dataTable_name, b.name as column_name, c.name as data_type, 
		b.max_length, b.precision, b.column_id,  b.is_identity, b.is_computed
	from sys.objects a join sys.columns b
		on b.object_id = a.object_id
		join sys.types c
			on c.user_type_id = b.user_type_id
	where a.type = 'u'
	and a.name = ?
	order by a.name, b.column_id`

	stmt, err := conn.Prepare(sql)
	if err != nil {
		log.Fatal("prepare failed:", err.Error())
	}

	defer stmt.Close()

	rows, err := stmt.Query(dataTableName)

	var column Column

	for rows.Next() {
		err = rows.Scan(&column.dataTable_name, &column.column_name, &column.data_type, &column.max_length, &column.precision,
			&column.column_id, &column.is_identity, &column.is_computed)
		if err != nil {
			log.Fatal("Scan Failed:", err.Error())
		}
		dataTable.columns = append(dataTable.columns, column)

	}

	return dataTable

}

// makeClassCode generates the code for a C# class to call the sprocs
func makeClassCode(dataTable DataTable) string {
	var buffer bytes.Buffer

	// header code
	buffer.WriteString(makeClassHeader(dataTable))

	// constructor - initialize the members, because the database is kinda funky
	// and doesn't handle null data very well
	buffer.WriteString(makeClassConstructor(dataTable))

	// getter / setter code
	buffer.WriteString(makeClassGetSets(dataTable))

	// save code 	-- it decides if it's an insert or update
	buffer.WriteString(makeClassSave(dataTable))

	// insert code
	buffer.WriteString(makeClassInsert(dataTable))

	// update code
	buffer.WriteString(makeClassUpdate(dataTable))

	// load parameters
	buffer.WriteString(makeClassParameters(dataTable))

	// delete code
	buffer.WriteString(makeClassDelete(dataTable))

	// load code -- based on identity key
	buffer.WriteString(makeClassLoad(dataTable))

	// loadFromRow()
	buffer.WriteString(makeClassLoadFromRow(dataTable))

	// footer code
	buffer.WriteString(makeClassFooter())
	buffer.WriteString(pp(3, fmt.Sprintf("Where is the sutff%s", "?")))
	return buffer.String()
}

// makeClassLoadFromRow generates the code to load class from a row
func makeClassLoadFromRow(dataTable DataTable) string {
	var buffer bytes.Buffer

	tl := 2

	buffer.WriteString(pp(tl, "public bool loadFromRow(DataRow row)\n"))
	buffer.WriteString(pp(tl, "{\n"))

	tl = 3

	buffer.WriteString(pp(tl, "bool bResult = false;\n\n"))

	for _, column := range dataTable.columns {
		buffer.WriteString(pp(tl, getClassDataAssignment(column)))
	}

	buffer.WriteString(pp(tl, "bResult = true;\n"))
	buffer.WriteString(pp(tl, "return bResult;\n"))
	buffer.WriteString(pp(tl-1, "}\n"))

	return buffer.String()
}

// makeClassLoad selects and loads a record based on the identity field
func makeClassLoad(dataTable DataTable) string {
	var buffer bytes.Buffer

	identity := getIdentityField(dataTable)
	tl := 2

	buffer.WriteString(pp(tl, "public bool Load()\n"))
	buffer.WriteString(pp(tl, "{\n"))

	tl = 3
	buffer.WriteString(pp(tl, "bool bResult = false;\n"))
	buffer.WriteString(pp(tl, "SqlConnection conn = getConnection();\n"))
	buffer.WriteString(pp(tl, "conn.Open();\n"))

	sproc := fmt.Sprintf("SqlCommand cmd = new SqlCommand(\"stp_%s_sel\", conn);\n", dataTable.name)
	buffer.WriteString(pp(tl, sproc))

	buffer.WriteString(pp(tl, "cmd.CommandType = CommandType.StoredProcedure;\n"))
	buffer.WriteString(pp(tl, fmt.Sprintf("cmd.Parameters.AddWithValue(\"@%s\", %s);\n\n", identity, identity)))
	buffer.WriteString(pp(tl, "DataTable dt = new DataTable();\n"))
	buffer.WriteString(pp(tl, "dt.Load(cmd.ExecuteReader());\n"))
	buffer.WriteString(pp(tl, "if (dt.Rows.Count > 0)\n"))
	buffer.WriteString(pp(tl+1, "bResult = loadFromRow(dt.Rows[0]);\n"))
	buffer.WriteString(pp(tl, "conn.Close();\n"))
	buffer.WriteString(pp(tl, "return bResult;\n"))
	buffer.WriteString(pp(tl, "}\n"))

	return buffer.String()
}

// makeClassDelete runs the delete sproc
func makeClassDelete(dataTable DataTable) string {
	var buffer bytes.Buffer

	identity := getIdentityField(dataTable)
	parmString := fmt.Sprintf("cmd.Parameters.AddWithValue(\"@%s\", %s);\n", identity, identity)

	tl := 2

	buffer.WriteString(pp(tl, "public void Delete()\n"))
	buffer.WriteString(pp(tl, "{\n"))
	tl = 3

	buffer.WriteString(pp(tl, "SqlConnection conn = getConnection();\n"))
	buffer.WriteString(pp(tl, "conn.Open();\n"))

	buffer.WriteString(pp(tl, fmt.Sprintf("SqlCommand cmd = new SqlCommand(\"stp_%s_del\", conn);\n", dataTable.name)))
	buffer.WriteString(pp(tl, "cmd.CommandType = CommandType.StoredProcedure;\n\n"))
	buffer.WriteString(pp(tl, parmString))
	buffer.WriteString(pp(tl, "cmd.ExecuteNonQuery();\n"))

	buffer.WriteString(pp(tl-1, "}\n\n"))

	return buffer.String()
}

//makeClassParameters will generate the parameters and append them to your cmd object
func makeClassParameters(dataTable DataTable) string {
	var buffer bytes.Buffer

	identity := getIdentityField(dataTable)

	tl := 2

	buffer.WriteString(pp(tl, "private void addParameters(SqlCommand cmd, bool isUpdate = false)\n"))
	buffer.WriteString(pp(tl, "{\n"))

	tl = 3

	buffer.WriteString(pp(tl, "if (isUpdate)\n"))
	buffer.WriteString(pp(tl+1, fmt.Sprintf("cmd.Parameters.AddWithValue(\"@%s\", %s);\n", identity, identity)))

	tempText := ""
	for _, column := range dataTable.columns {
		// we don't want to process any identity or computedcolumns.
		if !(column.is_identity || column.is_computed) {
			tempText = fmt.Sprintf("cmd.Parameters.AddWithValue(\"@%s\", %s);\n", column.column_name, column.column_name)
			buffer.WriteString(pp(tl, tempText))

		}
	}

	buffer.WriteString(pp(tl-1, "}\n\n"))

	return buffer.String()
}

// makeClassInsert will generate the code to call the insert sproc
func makeClassInsert(dataTable DataTable) string {
	var buffer bytes.Buffer

	tl := 2
	buffer.WriteString(pp(tl, "private int Insert()\n"))
	buffer.WriteString(pp(tl, "{\n"))
	tl = 3
	buffer.WriteString(pp(tl, "int iReturn = 0;\n"))
	buffer.WriteString(pp(tl, "SqlConnection conn = getConnection();\n"))
	buffer.WriteString(pp(tl, "conn.Open();\n"))
	buffer.WriteString(pp(tl, fmt.Sprintf("SqlCommand cmd = new SqlCommand(\"stp_%s_ins\", conn);\n", dataTable.name)))
	buffer.WriteString(pp(tl, "cmd.CommandType = CommandType.StoredProcedure;\n\n"))
	buffer.WriteString(pp(tl, "addParameters(cmd, false);\n\n"))
	buffer.WriteString(pp(tl, "iReturn = Convert.ToInt32(cmd.ExecuteScalar());\n"))
	buffer.WriteString(pp(tl, "return iReturn;\n"))

	buffer.WriteString(pp(tl-1, "}\n\n"))

	return buffer.String()
}

// makeClassUpdate will generate the code to call the update sproc
func makeClassUpdate(dataTable DataTable) string {
	var buffer bytes.Buffer

	tl := 2
	buffer.WriteString(pp(tl, "private int Update()\n"))
	buffer.WriteString(pp(tl, "{\n"))
	tl = 3
	buffer.WriteString(pp(tl, "int iReturn = 0;\n"))
	buffer.WriteString(pp(tl, "SqlConnection conn = getConnection();\n"))
	buffer.WriteString(pp(tl, "conn.Open();\n"))
	buffer.WriteString(pp(tl, fmt.Sprintf("SqlCommand cmd = new SqlCommand(\"stp_%s_upd\", conn);\n", dataTable.name)))
	buffer.WriteString(pp(tl, "cmd.CommandType = CommandType.StoredProcedure;\n\n"))
	buffer.WriteString(pp(tl, "addParameters(cmd, true);\n\n"))
	buffer.WriteString(pp(tl, "cmd.ExecuteNonQuery();\n"))
	buffer.WriteString(pp(tl, "return iReturn;\n"))

	buffer.WriteString(pp(tl-1, "}\n\n"))

	return buffer.String()
}

// makeClassFunctionDoc will create the summary comments that proceed a function
func makeClassFunctionDoc(tabs int, functionDesc string) string {
	var buffer bytes.Buffer

	buffer.WriteString(pp(tabs, "/// <summary>\n"))
	buffer.WriteString(pp(tabs, fmt.Sprintf("/// %s\n", functionDesc)))
	buffer.WriteString(pp(tabs, "/// </summary>\n"))
	buffer.WriteString(pp(tabs, "/// <returns></returns>\n"))

	return buffer.String()
}

func pp(tabs int, text string) string {

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s%s", strings.Repeat("\t", tabs), text))
	return buffer.String()

}

func getIdentityField(dataTable DataTable) string {

	for _, value := range dataTable.columns {
		if value.is_identity {
			return value.column_name
		}
	}
	return ""
}

// makeClassSave decides if we're inserting or updating and calls the right function
func makeClassSave(dataTable DataTable) string {
	var buffer bytes.Buffer

	identity := getIdentityField(dataTable)

	tl := 2

	buffer.WriteString(makeClassFunctionDoc(tl, "Save() will decide to call insert or update for you."))

	buffer.WriteString(pp(tl, "public int Save()\n"))
	buffer.WriteString(pp(tl, "{\n"))

	tl = 3

	buffer.WriteString(pp(tl, "int iReturn = 0;\n"))

	buffer.WriteString(pp(tl, fmt.Sprintf("if (%s > 0)\n", identity)))

	buffer.WriteString(pp(tl, "{\n"))

	tl = 4

	buffer.WriteString(pp(tl, "Update();\n"))

	buffer.WriteString(pp(tl, fmt.Sprintf("iReturn = %s;\n", identity)))
	tl = 3

	buffer.WriteString(pp(tl, "}\n"))
	buffer.WriteString(pp(tl, "else\n"))

	tl = 4
	buffer.WriteString(pp(tl, "iReturn = Insert();\n"))

	tl = 3
	buffer.WriteString(pp(tl, "return iReturn;\n"))

	tl = 2

	buffer.WriteString(pp(tl, "}\n"))

	return buffer.String()
}

// makeClassFooter writes some utility functions and closes the class
func makeClassFooter() string {
	var buffer bytes.Buffer

	buffer.WriteString("\t\tpublic SqlConnection getConnection() {\n\t\t\n")
	buffer.WriteString("\t\t\t")
	buffer.WriteString(fmt.Sprintf(`SqlConnection conn = Database.getSqlConnection("%s");`, *database))
	buffer.WriteString("\n\t\t\treturn conn;\n")
	buffer.WriteString("\t\t}\n")
	buffer.WriteString("\t}\n}")

	return buffer.String()
}

// getClassDataTypeDefault returns the varialbe initilizer
func getClassDataAssignment(column Column) string {
	name := column.column_name
	switch column.data_type {
	case "char", "varchar", "nvarchar", "nchar":
		return fmt.Sprintf("%s = row[\"%s\"].ToString();\n", name, name)
	case "text":
		return fmt.Sprintf("%s = row[\"%s\"].ToString();\n", name, name)
	case "smallint", "int", "bigint":
		return fmt.Sprintf("%s = Convert.ToInt32(row[\"%s\"]);\n", name, name)
	case "datetime", "smalldatetime":
		return fmt.Sprintf("%s = Convert.ToDateTime(row[\"%s\"].ToString());\n", name, name)
	case "bit":
		return fmt.Sprintf("%s = Convert.ToBoolean(row[\"%s\"].ToString());\n", name, name)
	case "decimal", "numeric":
		return fmt.Sprintf("%s = Convert.ToDecimal(row[\"%s\"].ToString());\n", name, name)
	case "float":
		return fmt.Sprintf("%s = Convert.ToFloat(row[\"%s\"].ToString());\n", name, name)
	}
	return ""
}

// getClassDataTypeDefault returns the varialbe initilizer
func getClassDataTypeDefault(column Column) string {
	switch column.data_type {
	case "char", "varchar", "nvarchar", "nchar":
		return "string.Empty"
	case "text":
		return "string.Empty" // TODO: fix this for reals, ya'll
	case "smallint", "int", "bigint":
		return "0"
	case "datetime", "smalldatetime":
		return `DateTime.Parse("1/1/1900")`
	case "bit":
		return "false"
	case "decimal", "numeric":
		return "0.0"
	case "float":
		return "0.0"
	}
	return "string.Empty"
}

// getClassDataType returns the C# data type for a sql data type
func getClassDataType(column Column) string {

	switch column.data_type {
	case "char", "varchar", "nvarchar", "nchar":
		return "string"
	case "text":
		return "string" // TODO: fix this for reals, ya'll
	case "smallint", "int", "bigint":
		return "int"
	case "datetime", "smalldatetime":
		return "DateTime"
	case "bit":
		return "bool"
	case "decimal", "numeric":
		return "decimal"
	case "float":
		return "float"

	}
	return "string"
}

// makeClasConstructor generates the constructor code for the class
func makeClassConstructor(dataTable DataTable) string {
	var buffer bytes.Buffer

	var tl = "\t\t" // the tab level that we're currently at
	buffer.WriteString(fmt.Sprintf("%spublic %s()\n", tl, dataTable.name))
	buffer.WriteString(fmt.Sprintf("%s{\n", tl))
	tl = "\t\t\t"

	for _, value := range dataTable.columns {
		// we don't want to process any identity columns.
		if !value.is_identity {
			buffer.WriteString(fmt.Sprintf("%s%s = %s;\n", tl, value.column_name, getClassDataTypeDefault(value)))
		}
	}
	tl = "\t\t"
	buffer.WriteString(fmt.Sprintf("%s}\n\n", tl))
	return buffer.String()
}

// makeClassGetSets generates the gettors and settors for the class
func makeClassGetSets(dataTable DataTable) string {
	var buffer bytes.Buffer
	var tl = "\t\t"

	for _, value := range dataTable.columns {
		buffer.WriteString(fmt.Sprintf("%spublic %s %s { get; set; }\n", tl, getClassDataType(value), value.column_name))
	}
	buffer.WriteString("\n")
	return buffer.String()
}

// makeClassHeader generates the header information for the class
func makeClassHeader(dataTable DataTable) string {
	var buffer bytes.Buffer

	buffer.WriteString("using System;\nusing System.Collections.Generic;\nusing System.Data;\n")
	buffer.WriteString("using System.Data.SqlClient;\nusing FECUtil;\n\n")

	buffer.WriteString(fmt.Sprintf("namespace %s {\n", *database))

	functionDoc := fmt.Sprintf("this class is used for all common functionality for a record in the\n\t/// %s dataTable in the %s database on the %s server\n", *table, *database, *server)

	// TODO: convert to pp() here
	buffer.WriteString(makeClassFunctionDoc(1, functionDoc))

	buffer.WriteString(fmt.Sprintf("\tpublic class %s\n\t{\n", dataTable.name))

	return buffer.String()
}

// makeSqlCode generates the code for Sql Server sprocs
func makeSqlCode(dataTable DataTable) string {
	var buffer bytes.Buffer

	// write the use clause
	buffer.WriteString(fmt.Sprintf("use %s\n\n", *database))

	// insert sproc
	buffer.WriteString("\n-- ******** INSERT ********\n")
	buffer.WriteString(makeSqlInsert(dataTable))

	// update sproc
	buffer.WriteString("\n-- ******** UPDATE ********\n")
	buffer.WriteString(makeSqlUpdate(dataTable))

	// delete sproc
	buffer.WriteString("\n-- ******** DELETE ********\n")
	buffer.WriteString(makeSqlDelete(dataTable))

	// select sproc
	buffer.WriteString("\n-- ******** READ ********\n")
	buffer.WriteString(makeSqlSelect(dataTable))

	return buffer.String()

}

// makeSqlDrop generates the code to drop a sproc if it exists
func makeSqlDropStatement(sprocName string) string {
	sql := "if exists (select name from sysobjects where name = '%s')\n\tdrop proc %s\ngo"
	return fmt.Sprintf(sql, sprocName, sprocName)

}

// makeSqlUpdate returns the text for creating an update sproc
func makeSqlUpdate(dataTable DataTable) string {
	var buffer bytes.Buffer

	parameters := make([]string, 0)
	fields := make([]string, 0)
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_upd", dataTable.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// loop through the columns and create the SET clause and the WHERE clause
	for _, value := range dataTable.columns {
		// we don't want to process any computed columns.
		if !value.is_computed {
			metaData := getMetaData(value)
			parameters = append(parameters, "\t"+metaData)
			if !value.is_identity {
				fields = append(fields, fmt.Sprintf("%s = @%s", value.column_name, value.column_name))
			} else {
				whereClause = fmt.Sprintf("WHERE %s = @%s", value.column_name, value.column_name)
			}
		}
	}

	buffer.WriteString(strings.Join(parameters, ",\n"))

	buffer.WriteString("\nAS\n")

	buffer.WriteString(fmt.Sprintf("update %s\n", dataTable.name))
	buffer.WriteString(fmt.Sprintf("SET %s", strings.Join(fields, ", ")))
	buffer.WriteString(fmt.Sprintf("\n%s", whereClause))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

// makeSqlDelete returns the text for creating a delete sproc
func makeSqlDelete(dataTable DataTable) string {
	var buffer bytes.Buffer

	var parameter string
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_del", dataTable.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range dataTable.columns {
		// this is an insert clause, so skip the ones that are an identity column
		// don't write directly to the buffer - we end up with the
		// 'too many commas' problem. Simpler to use a strings.join
		// than to try to remove the last comma.
		if value.is_identity {
			metaData := getMetaData(value)
			parameter = "\t" + metaData
			whereClause = fmt.Sprintf("WHERE %s = @%s", value.column_name, value.column_name)

		}
	}

	buffer.WriteString(parameter)

	buffer.WriteString("\nAS\n")

	buffer.WriteString(fmt.Sprintf("DELETE FROM %s\n", dataTable.name))
	buffer.WriteString(fmt.Sprintf("\n%s", whereClause))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

// makeSqlSelect returns the text for creating a select sproc
// it assumes a identity field as a primary key
func makeSqlSelect(dataTable DataTable) string {
	var buffer bytes.Buffer

	fields := make([]string, 0)
	var parameter string
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_sel", dataTable.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range dataTable.columns {
		// this is an insert clause, so skip the ones that are an identity column
		// don't write directly to the buffer - we end up with the
		// 'too many commas' problem. Simpler to use a strings.join
		// than to try to remove the last comma.
		if !value.is_computed {
			if value.is_identity {
				parameter = getMetaData(value)
				whereClause = fmt.Sprintf("WHERE %s = @%s", value.column_name, value.column_name)
			} else {
				fields = append(fields, fmt.Sprintf("%s", value.column_name))
			}
		}
	}

	buffer.WriteString(fmt.Sprintf("\t%s", parameter))
	buffer.WriteString("\nAS\n")

	buffer.WriteString(fmt.Sprintf("SELECT %s\n", strings.Join(fields, ", ")))
	buffer.WriteString(fmt.Sprintf("FROM %s\n", dataTable.name))
	buffer.WriteString(whereClause)
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

// makeSqlInsert returns the text for creating an insert sproc
func makeSqlInsert(dataTable DataTable) string {
	var buffer bytes.Buffer

	parameters := make([]string, 0)
	fields := make([]string, 0)
	parms := make([]string, 0) // could be created by manipulating fields, but these are so small it doesn't matter
	var outputParm string
	var outputParmName string

	sprocName := fmt.Sprintf("stp_%s_ins", dataTable.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))

	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range dataTable.columns {
		// this is an insert clause, so skip the ones that are an identity column
		// don't write directly to the buffer - we end up with the
		// 'too many commas' problem. Simpler to use a strings.join
		// than to try to remove the last comma.
		if !(value.is_identity || value.is_computed) {
			metaData := getMetaData(value)
			parameters = append(parameters, "\t"+metaData)
			fields = append(fields, value.column_name)
			parms = append(parms, "@"+value.column_name)
		} else {
			if value.is_identity {
				outputParmName = "@" + value.column_name
				outputParm = fmt.Sprintf("%s OUTPUT", getMetaData(value))
			}
		}
	}

	buffer.WriteString(strings.Join(parameters, ",\n"))
	buffer.WriteString(fmt.Sprintf(",\n\t%s", outputParm))
	buffer.WriteString("\nAS\n")
	buffer.WriteString(fmt.Sprintf("insert into %s (%s)\n", dataTable.name, strings.Join(fields, ", ")))
	buffer.WriteString(fmt.Sprintf("\nVALUES (%s)", strings.Join(parms, ", ")))
	buffer.WriteString(fmt.Sprintf("\nSET %s = scope_identity()", outputParmName))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}
