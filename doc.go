package asc

/*

Package asc - Aersopike datastore manager factory

This library comes with the aerospike datastore manager implementing datastore connectivity manager (dsc)
Is uses native aerospike client to connect to NoSQL Aerospike server, and adds SQL layer on top of it.

Usage:


import (
    _ "github.com/aerospike/aerospike-client-go"
    _ "github.com/viant/asc"
)

{
 	config := dsc.NewConfig("aerospike", "", "host:127.0.0.1,port:3000,namespace:test,generationColumnName:generation,dateLayout:2006-01-02 15:04:05.000")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
}

*/

