package asc_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestNewConnection(t *testing.T) {
	config := dsc.NewConfig("aerospike", "", "host:104.197.250.82,port:34143,namespace:test,generationColumnName:generation,dateLayout:2006-01-02 15:04:05.000,connectionTimeout:1000")
	factory := dsc.NewManagerFactory()
	manager, _ := factory.Create(config)
	provider := manager.ConnectionProvider()
	_, err := provider.NewConnection()
	assert.NotNil(t, err)
}
