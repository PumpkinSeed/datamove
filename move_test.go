package datamove

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var connStr = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"

func TestFetch(t *testing.T) {
	db, err := Connect(Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "test",
	})
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	//var e = Employers{}
	data, err := Fetch(db, Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "test",
	}, "")
	if err != nil {
		t.Error(err)
	}

	lala, _ := json.Marshal(data)
	fmt.Println(string(lala))

	dests := Database{
		Driver: "mysql",
		Conn: "root:test12345@tcp(100.113.104.12:3306)/employer?parseTime=true",
		TableName: "EmployerBases",
	}
	destconn, err := Connect(dests)
	if err != nil {
		t.Error(err)
	}

	for _, row := range data {
		delete(row, "registrationCourt")
		if v, ok := row["status"].(string); ok {
			if v == "draft" {
				row["status"] = "inactive"
			}
		}
	}

	err = Load(destconn, dests, data)
	if err != nil {
		t.Error(err)
	}
}

func TestMove(t *testing.T) {
	mes := time.Now()
	err := Move(Settings{
		Destination: Database{"mysql", connStr, "users2"},
		Source:      Database{"mysql", connStr, "users"},
	})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(time.Since(mes))
}

func TestBuildInsert(t *testing.T) {
	db, err := Connect(Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "users",
	})
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	result, err := Fetch(db, Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "users",
	}, "")
	if err != nil {
		t.Error(err)
	}

	insert := buildInsert("users", result)
	fmt.Println(insert)
}

func BenchmarkFetch(b *testing.B) {
	db, err := Connect(Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "users",
	})
	if err != nil {
		b.Error(err)
	}
	defer db.Close()

	for i := 0; i < b.N; i++ {
		_, err := Fetch(db, Database{
			Driver:    "mysql",
			Conn:      connStr,
			TableName: "users",
		}, "")
		if err != nil {
			b.Error(err)
		}
	}
}
