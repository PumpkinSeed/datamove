package datamove

import (
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Settings struct {
	Source                 Database
	Destination            Database
	CreateDestinationTable bool
}

type Database struct {
	Driver    string
	Conn      string
	TableName string
}

func Move(s Settings) error {
	destConn, err := Connect(s.Destination)
	if err != nil {
		return err
	}

	srcConn, err := Connect(s.Source)
	if err != nil {
		return err
	}

	data, err := Fetch(srcConn, s.Source)
	if err != nil {
		return err
	}

	if s.CreateDestinationTable {
		// @TODO
	}

	return Load(destConn, s.Destination, data)
}

func Connect(d Database) (*sqlx.DB, error) {
	return sqlx.Open(d.Driver, d.Conn)
}

func Fetch(db *sqlx.DB, d Database) ([]map[string]interface{}, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	var sqlQ = fmt.Sprintf("SELECT * FROM %s", d.TableName)
	rows, err := db.Query(sqlQ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resultSet []map[string]interface{}
	cols, _ := rows.Columns()
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = *val
		}

		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		resultSet = append(resultSet, m)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return resultSet, nil
}

func Load(db *sqlx.DB, d Database, data []map[string]interface{}) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	insertStmt := buildInsert(d.TableName, data)

	for _, row := range data {
		if _, err := tx.NamedExec(insertStmt, row); err != nil {
			if err := tx.Rollback(); err != nil {
				return err
			}
			return err
		}
	}
	tx.Commit()

	return nil
}

func buildInsert(tableName string, data []map[string]interface{}) string {
	template := "INSERT INTO %s (%s) VALUES (%s);"
	if len(data) > 0 {
		var cols []string
		var dataFields []string
		for col := range data[0] {
			cols = append(cols, col)
			dataFields = append(dataFields, fmt.Sprintf(":%s", col))
		}

		return fmt.Sprintf(
			template,
			tableName,
			strings.Join(cols, ", "),
			strings.Join(dataFields, ", "),
		)
	}
	return ""
}
