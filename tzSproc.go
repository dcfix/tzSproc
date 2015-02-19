package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
	"strings"
)

var debug = flag.Bool("debug", false, "enable debugging")
var server = flag.String("server", "fecsql03", "the database server")
var database = flag.String("database", "internal", "the database ")
var table = flag.String("table", "", "the database table")
var user = flag.String("user", "SPWebProg", "the database user")
var password = flag.String("password", "", "the user password")
var port = flag.Int("port", 1433, "the database port")

type Table struct {
	name    string
	columns []Column
}

type Column struct {
	table_name  string
	column_name string
	data_type   string
	max_length  int
	precision   int
	column_id   int
	is_identity bool
	is_computed bool
}

func main() {
	flag.Parse() // parse the command line args
	processTable("EmployeeIT")
}

func getConnectionString() string {
	connString := fmt.Sprintf("server=%s;port=%d;database=%s;user=%s;password=%s", *server, *port, *database, *user, *password)
	return connString
}

func loadTable(tableName string) Table {
	table := Table{}
	table.name = tableName

	connString := getConnectionString()
	conn, err := sql.Open("mssql", connString)

	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	sql := `select a.name as table_name, b.name as column_name, c.name as data_type, 
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

	rows, err := stmt.Query(tableName)

	var column Column

	for rows.Next() {
		err = rows.Scan(&column.table_name, &column.column_name, &column.data_type, &column.max_length, &column.precision,
			&column.column_id, &column.is_identity, &column.is_computed)
		if err != nil {
			log.Fatal("Scan Failed:", err.Error())
		}
		table.columns = append(table.columns, column)

	}

	return table

}

func makeSqlCode(table Table) string {
	var buffer bytes.Buffer

	// insert sproc
	buffer.WriteString("\n-- ******** INSERT ********\n")
	buffer.WriteString(makeSqlInsert(table))

	// update sproc
	buffer.WriteString("\n-- ******** UPDATE ********\n")
	buffer.WriteString(makeSqlUpdate(table))

	// delete sproc
	buffer.WriteString("\n-- ******** DELETE ********\n")
	buffer.WriteString(makeSqlDelete(table))

	// select sproc
	buffer.WriteString("\n-- ******** READ ********\n")
	buffer.WriteString(makeSqlSelect(table))

	return buffer.String()

}

func makeSqlDropStatement(sprocName string) string {
	sql := "if exists (select name from sysobjects where name = '%s')\n\tdrop proc %s\ngo"
	return fmt.Sprintf(sql, sprocName, sprocName)

}

func makeSqlUpdate(table Table) string {
	var buffer bytes.Buffer

	parameters := make([]string, 0)
	fields := make([]string, 0)
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_upd", table.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range table.columns {
		// this is an insert clause, so skip the ones that are an identity column
		// don't write directly to the buffer - we end up with the
		// 'too many commas' problem. Simpler to use a strings.join
		// than to try to remove the last comma.
		if !value.is_computed {
			metaData := getMetaData(value)
			parameters = append(parameters, "\t"+metaData)
			if !value.is_identity {
				fields = append(fields, fmt.Sprintf("%s=@%s", value.column_name, value.column_name))
			} else {
				whereClause = fmt.Sprintf("\nWHERE %s = @%s", value.column_name, value.column_name)
			}
		}
	}

	buffer.WriteString(strings.Join(parameters, ",\n"))

	buffer.WriteString("\nAS\n")

	buffer.WriteString(fmt.Sprintf("update %s\n", table.name))
	buffer.WriteString(fmt.Sprintf("SET %s", strings.Join(fields, ", ")))
	buffer.WriteString(fmt.Sprintf("\n%s", whereClause))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

func makeSqlDelete(table Table) string {
	var buffer bytes.Buffer

	var parameter string
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_del", table.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range table.columns {
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

	buffer.WriteString(fmt.Sprintf("DELETE FROM %s\n", table.name))
	buffer.WriteString(fmt.Sprintf("\n%s", whereClause))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

func makeSqlSelect(table Table) string {
	var buffer bytes.Buffer

	fields := make([]string, 0)
	var parameter string
	var whereClause string

	sprocName := fmt.Sprintf("stp_%s_sel", table.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))
	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range table.columns {
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
	buffer.WriteString(fmt.Sprintf("FROM %s\n", table.name))
	buffer.WriteString(whereClause)
	buffer.WriteString("\ngo\n")
	return buffer.String()

}

func makeSqlInsert(table Table) string {
	var buffer bytes.Buffer

	parameters := make([]string, 0)
	fields := make([]string, 0)
	parms := make([]string, 0) // could be created by manipulating fields, but these are so small it doesn't matter
	var outputParm string
	var outputParmName string

	sprocName := fmt.Sprintf("stp_%s_ins", table.name)

	buffer.WriteString(makeSqlDropStatement(sprocName))

	buffer.WriteString(fmt.Sprintf("\nCREATE proc %s \n", sprocName))

	// print the parameters
	for _, value := range table.columns {
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
	buffer.WriteString(fmt.Sprintf("insert into %s (%s)\n", table.name, strings.Join(fields, ", ")))
	buffer.WriteString(fmt.Sprintf("\nVALUES (%s)", strings.Join(parms, ", ")))
	buffer.WriteString(fmt.Sprintf("\nSET %s = scope_identity()", outputParmName))
	buffer.WriteString("\ngo\n")
	return buffer.String()

}
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

func makeClassCode(table Table) string {
	var buffer bytes.Buffer

	// header code

	// getter / setter code

	// insert code

	// update code

	// delete code

	// read code -- based on identity key

	// loadFromRow()

	// footer code
	return buffer.String()
}

func processTable(tableName string) {
	table := loadTable(tableName)

	sprocs := makeSqlCode(table)
	class := makeClassCode(table)

	fmt.Printf(sprocs)
	fmt.Printf(class)
}
