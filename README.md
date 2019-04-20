# Datastore Connectivity for Aerospike (asc)


[![Datastore Connectivity library for Aerospike in Go.](https://goreportcard.com/badge/github.com/viant/asc)](https://goreportcard.com/report/github.com/viant/asc)
[![GoDoc](https://godoc.org/github.com/viant/asc?status.svg)](https://godoc.org/github.com/viant/asc)

This library is compatible with Go 1.11+


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)


#### Configuration parameters

###### aerospike client/policy config params
 - timeoutMs
 - connectionTimeout
 - serverSocketTimeout
 - scanPct
 - host
 - port
 - namespace
 - sleepBetweenRetries
 - batchSize
 
 
###### keyColumn, keyColumnName
 
Defines name of column used as record key ('id' by default)

It can be specified per table i.e

    events.keyColumn = code
    

###### excludedColumns

List of columns to be excluded from record (i.e: id - in case we need it only as record key)


###### dateFormat

ISO date format used to time.Time conversion


###### optimizeLargeScan

Experimental feature that first scan all keys and write then to disk
and then separate go routines scan data using the dumped keys

You can only specify _scanBaseDirectory_


## Usage:


The following is a very simple example of CRUD operations with dsc

config.yaml
```yaml
driverName: aerospike
parameters:
  namespace: test
  host: 127.0.0.1
  dateFormat: yyyy-MM-dd hh:mm:ss
```

```go
package main

import (
    _ "github.com/aerospike/aerospike-client-go"
    _ "github.com/viant/asc"
    "github.com/viant/dsc"
    "log"
)


type Interest struct {
	Id int	`autoincrement:"true"`
	Name string
	ExpiryTimeInSecond int `column:"expiry"`
	Category string
}


func main() {


	config, err := dsc.NewConfigFromURL("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		log.Fatal(err)
	}



  	// manager := factory.CreateFromURL("file:///etc/myapp/datastore.json")
  
    interest := &Interest{}
    
    success, err:= manager.ReadSingle(interest, "SELECT id, name, expiry, category FROM interests WHERE id = ?", []interface{}{id},nil)
	if err != nil {
        panic(err.Error())
	}

    var intersts = make([]*Interest, 0)
    err = manager.ReadAll(&intersts, "SELECT id, name, expiry, category FROM interests", nil ,nil)
    if err != nil {
        panic(err.Error())
    }

    
    intersts = []*Interest {
        Interest{Name:"Abc", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Name:"Def", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Id:"20, Name:"Ghi", ExpiryTimeInSecond:3600, Category:"xyz"},
    }


	_, _, err = manager.PersistAll(&intersts, "intersts", nil)
	if err != nil {
        panic(err.Error())
   	}
   	fmt.Printf("Inserted %v, updated: %v\n", inserted, updated)
  
    deleted, err := manager.DeleteAll(&intersts, "intersts", nil)
    if err != nil {
        panic(err.Error())
   	}
 	fmt.Printf("Inserted %v, updated: %v\n", deleted)


 	
 	var records = []map[string]interface{}{}
 	 err = manager.ReadAll(&records, "SELECT id, name, expiry, category FROM interests", nil ,nil)
    if err != nil {
        panic(err.Error())
    }
}
```


### Query level UDF support

- **ARRAY** converts supplied source column into collection of map entry defined as (key, value) 

```sql
SELECT 
  id, 
  username, 
ARRAY(city_visited) AS visited
FROM users
```


- **JSON**  convert supplied source column to JSON
```sql
SELECT 
  id, 
  username, 
JSON(city_visited) AS visited
FROM users
```

## GoCover

[![GoCover](https://gocover.io/github.com/viant/asc)](https://gocover.io/github.com/viant/asc)


<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:**