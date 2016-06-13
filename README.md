# Datastore Connectivity for Aerospike (asc)

Datastore Connectivity library for Aerospike in Go.

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)





## Usage:


The following is a very simple example of CRUD operations with dsc

```go
package main

import (
    _ "github.com/aerospike/aerospike-client-go"
    _ "github.com/viant/asc"
)


type Interest struct {
	Id int	`autoincrement:"true"`
	Name string
	ExpiryTimeInSecond int `column:"expiry"`
	Category string
}


func main() {

	config := dsc.NewConfig("aerospike", "", "host:127.0.0.1,port:3000,namespace:test,generationColumnName:generation,dateLayout:2006-01-02 15:04:05.000")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
    if err != nil {
            panic(err.Error())
    }

  	// manager := factory.CreateFromURL("file:///etc/myapp/datastore.json")
  
    interest := Interest{}
    
    success, err:= manager.ReadSingle(&interest, "SELECT id, name, expiry, category FROM interests WHERE id = ?", []interface{}{id},nil)
	if err != nil {
        panic(err.Error())
	}

    var intersts = make([]Interest, 0)
    err:= manager.ReadAll(&interests, "SELECT id, name, expiry, category FROM interests", nil ,nil)
    if err != nil {
        panic(err.Error())
    }

    
    intersts := []Interest {
        Interest{Name:"Abc", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Name:"Def", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Id:"20, Name:"Ghi", ExpiryTimeInSecond:3600, Category:"xyz"},
    }


	inserted, updated, err:= manager.PersistAll(&intersts, "intersts", nil)
	if err != nil {
        panic(err.Error())
   	}
   	fmt.Printf("Inserted %v, updated: %v\n", inserted, updated)
  
    deleted, err := manager.DeleteAll(&intersts, "intersts", nil)
    if err != nil {
        panic(err.Error())
   	}
 	fmt.Printf("Inserted %v, updated: %v\n", deleted)
  
}
```



<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:**