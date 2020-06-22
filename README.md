# datamove

**Migrate data easily.**

Features:
- Help to migrate a table to another even across databases.
- Custom field manipulation or transformation.

## Usage

#### Migration without data transform

```go
package main

import "gtihub.com/PumpkinSeed/datamove"

func main() {
    connStr := "username:password@tcp(127.0.0.1:3306)/dbname?parseTime=true"
    err := datamove.Move(datamove.Settings{
    	Destination: datamove.Database{"mysql", connStr, "users2"},
    	Source: datamove.Database{"mysql", connStr, "users"},
    })
    if err != nil {
        panic(err)
    }
}
```

#### Migration with data transform

```go
package main

import "gtihub.com/PumpkinSeed/datamove"

func main() {
    connStr := "username:password@tcp(127.0.0.1:3306)/dbname?parseTime=true"
    settings := datamove.Settings{
        Destination: datamove.Database{"mysql", connStr, "users2"},
        Source: datamove.Database{"mysql", connStr, "users"},
    }
    srcdb, err := datamove.Connect(settings.Source)
    if err != nil {
        panic(err)
    } 
    data, err := datamove.Fetch(srcdb, settings.Source)
    if err != nil {
        panic(err)
    }

    // DO THE TRANSFORM ON THE data
    
    destdb, err := datamove.Connect(settings.Destination)
    if err != nil {
        panic(err)
    }
    if err := datamove.Load(destdb, settings.Destination, data); err != nil {
        panic(err)
    }
}
```

## TODO

- Create table option
