package datamove

import (
	"fmt"
	"testing"
	"time"
)

func TestFetch(t *testing.T) {
	connStr := "fluidpay:fluidpay@tcp(127.0.0.1:3306)/fluidpay?parseTime=true"
	db, err := Connect(Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "users",
	})
	if err != nil {
		t.Error(err)
	}
	defer db.Close()


	_, err = Fetch(db, Database{
		Driver:    "mysql",
		Conn:      connStr,
		TableName: "users",
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMove(t *testing.T) {
	mes := time.Now()
	connStr := "fluidpay:fluidpay@tcp(127.0.0.1:3306)/fluidpay?parseTime=true"
	err := Move(Settings{
		Destination: Database{"mysql", connStr, "users2"},
		Source: Database{"mysql", connStr, "users"},
	})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(time.Since(mes))
}

func TestBuildInsert(t *testing.T) {
	connStr := "fluidpay:fluidpay@tcp(127.0.0.1:3306)/fluidpay?parseTime=true"
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
	})
	if err != nil {
		t.Error(err)
	}

	insert := buildInsert("users", result)
	fmt.Println(insert)
}


func BenchmarkFetch(b *testing.B) {
	connStr := "fluidpay:fluidpay@tcp(127.0.0.1:3306)/fluidpay?parseTime=true"
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
		})
		if err != nil {
			b.Error(err)
		}
	}
}
