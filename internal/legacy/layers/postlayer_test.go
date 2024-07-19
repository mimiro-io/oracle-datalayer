package layers

import (
	"database/sql"
	"fmt"
	"github.com/franela/goblin"
	go_ora "github.com/sijms/go-ora/v2"

	"testing"
)

func TestEncodeLine(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Entitiy converted to rows", func() {
		g.It("Should return statements for batch insert", func() {

			oracleUrl := go_ora.BuildUrl("localhost", 1521, "ORCL", "animal", "test1234", map[string]string{"SERVER": "localhost:"})

			fmt.Println(oracleUrl)
			input := `{"id":"a:1", "deleted": false, "refs":{}, "props":{"a:id": "a:1", "a:name": "Hank"}}`
			expected := "INSERT INTO TABLE PEOPLE('id','name') VALUES (1,'Hank');"
			fmt.Println(input)
			fmt.Println(expected)
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
