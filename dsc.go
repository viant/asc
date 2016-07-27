package asc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("aerospike", newManagerFactory())
	dsc.RegisterDatastoreDialect("aerospike", newDialect())
}

func init() {
	register()
}
