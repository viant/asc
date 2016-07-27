package asc_test

import (
	"testing"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/stretchr/testify/assert"
)

func TestDialectGetDatastores(t *testing.T) {
	factory := dsc.NewManagerFactory()
	configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded( "test://test/config/store.json")
	manager, err := factory.CreateFromURL(configUrl)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialect("aerospike")

	names, err :=dialect.GetDatastores(manager)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(names))
}



func TestDialectGetDatastore(t *testing.T) {
	factory := dsc.NewManagerFactory()
	configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded( "test://test/config/store.json")
	manager, err := factory.CreateFromURL(configUrl)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialect("aerospike")
	name, err :=dialect.GetCurrentDatastore(manager)
	assert.Nil(t, err)
	assert.Equal(t, "test", name)
}



func TestDialectGetTables(t *testing.T) {
	factory := dsc.NewManagerFactory()
	configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store1.json")
	manager, err := factory.CreateFromURL(configUrl)
	assert.Nil(t, err)

	assert.NotNil(t, manager)

	dialect := dsc.GetDatastoreDialect("aerospike")
	_, err =dialect.GetTables(manager, "test")
	assert.Nil(t, err)

}

