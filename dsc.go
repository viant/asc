package asc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("aerospike", newManagerFactory())
	dsc.RegisterDatastoreDialectable("aerospike", newDialect())
}

func init() {
	register()
}
