package layers

import (
	"database/sql"
	"fmt"
	"github.com/franela/goblin"
	go_ora "github.com/sijms/go-ora/v2"

	"os"
	"testing"
)

func TestDecodeLine(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Is connection to db working?", func() {
		g.Xit("Should return sql.Conn", func() {

			oracleUrl := go_ora.BuildUrl("localhost", 1521, "ORCL", "animal", "test1234", map[string]string{"SERVER": "localhost:"})

			fmt.Println(oracleUrl)

			db, err := sql.Open("oracle", oracleUrl)
			if err != nil {
				panic(fmt.Errorf("error in sql.Open: %w", err))
			}
			defer func() {
				err = db.Close()
				if err != nil {
					fmt.Println("Can't close connection: ", err)
				}
			}()
			query2 := "select * from ANIMAL.ANIMAL_TABLE"
			query := "select table_name from all_tables where tablespace_name is not null"
			stmt, err := db.Prepare(query2)
			//err = db.Ping()
			if err != nil {
				fmt.Printf("db.Prepare(%s) failed. \n\t%s\n", query, err.Error())
			}
			defer stmt.Close()
			rows, err := stmt.Query()
			if err != nil {
				fmt.Printf("stmt.Query() failed. \n\t%s\n", err.Error())
			}
			defer rows.Close()

			var columns []string
			columns, err = rows.Columns()
			for i, c := range columns {
				fmt.Printf("%3d %s\n", i, c)
			}

			for rows.Next() {
				var sysdate string
				err = rows.Scan(&sysdate)
				if err != nil {
					fmt.Printf("rows.Scan(...) failed.\n\t%s\n", err.Error())

				}
				fmt.Println("SYSDATE:", sysdate)
			}
			err = rows.Err()
			if err != nil {
				fmt.Printf("rows iteration failed.\n\t%s\n", err.Error())

			}
			someAdditionalActions(db)

			g.Assert(err).IsNil()
		})
	})
}

func someAdditionalActions(db *sql.DB) {

	var queryResultColumnOne string
	row := db.QueryRow("SELECT systimestamp FROM dual")
	err := row.Scan(&queryResultColumnOne)
	if err != nil {
		panic(fmt.Errorf("error scanning db: %w", err))
	}
	fmt.Println("The time in the database ", queryResultColumnOne)
	_, err = db.Exec(createTableStatement)
	handleError("create table", err)
	defer db.Exec(dropTableStatement)
	stmt, err := db.Prepare(insertStatement)
	handleError("prepare insert statement", err)
	sqlresult, err := stmt.Exec("John", 42)
	handleError("execute insert statement", err)
	rowCount, _ := sqlresult.RowsAffected()
	fmt.Println("Inserted number of rows = ", rowCount)

	var queryResultName string
	var queryResultTimestamp string
	var queryResultValue int32
	row = db.QueryRow("SELECT name, creation_time, value FROM temp_table")
	err = row.Scan(&queryResultName, &queryResultTimestamp, &queryResultValue)
	handleError("query single row", err)
	if err != nil {
		panic(fmt.Errorf("error scanning db: %w", err))
	}
	fmt.Println(fmt.Sprintf("The name: %s, time: %s, value:%d ", queryResultName, queryResultTimestamp, queryResultValue))

	_, err = stmt.Exec("Jane", 69)
	handleError("execute insert statement", err)
	_, err = stmt.Exec("Malcolm", 13)
	handleError("execute insert statement", err)

	// fetching multiple rows
	theRows, err := db.Query("select name, value from TEMP_TABLE")
	handleError("Query for multiple rows", err)
	defer theRows.Close()
	var (
		name  string
		value int32
	)
	for theRows.Next() {
		err := theRows.Scan(&name, &value)
		handleError("next row in multiple rows", err)
		fmt.Println(fmt.Sprintf("The name: %s and value:%d ", name, value))
	}
	err = theRows.Err()
	handleError("next row in multiple rows", err)

}

const createTableStatement = "CREATE TABLE TEMP_TABLE ( NAME VARCHAR2(100), CREATION_TIME TIMESTAMP DEFAULT SYSTIMESTAMP, VALUE  NUMBER(5))"
const dropTableStatement = "DROP TABLE TEMP_TABLE PURGE"
const insertStatement = "INSERT INTO TEMP_TABLE ( NAME , VALUE) VALUES (:name, :value)"

func handleError(msg string, err error) {
	if err != nil {
		fmt.Println(msg, err)
		os.Exit(1)
	}
}
