package datamove

import (
	"errors"
	"fmt"
	"reflect"
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

	data, err := Fetch(srcConn, s.Source, "")
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

func Fetch(db *sqlx.DB, d Database, data interface{}) ([]map[string]interface{}, error) {
	// @TODO check data is a pointer
	if db == nil {
		return nil, errors.New("db is nil")
	}

	var resultSet []map[string]interface{}
	var sqlQ = fmt.Sprintf("SELECT * FROM %s", d.TableName)
	rows, err := db.Queryx(sqlQ)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.StructScan(data)
		if err != nil {
			return nil, err
		}
		rv := reflect.Indirect(reflect.ValueOf(data))
		rt := rv.Type()
		var resultSetSub = make(map[string]interface{})
		for i := 0; i < rt.NumField(); i++ {
			if v, ok := rt.Field(i).Tag.Lookup("db"); !ok {
				return nil, errors.New("missing db field tag")
			} else {
				resultSetSub[v] = rv.Field(i).Interface()
			}
		}
		resultSet = append(resultSet, resultSetSub)
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
