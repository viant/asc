package asc_test

//Refactoring ...
//
//import (
//	"github.com/stretchr/testify/assert"
//	_ "github.com/viant/asc"
//	"github.com/viant/dsc"
//	"github.com/viant/dsunit"
//	"testing"
//)
//
//func TestDialectGetDatastores(t *testing.T) {
//	factory := dsc.NewManagerFactory()
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//		dialect := dsc.GetDatastoreDialect("aerospike")
//
//		names, err := dialect.GetDatastores(manager)
//		assert.Nil(t, err)
//		assert.Equal(t, 1, len(names))
//	}
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store2.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//		dialect := dsc.GetDatastoreDialect("aerospike")
//		_, err = dialect.GetDatastores(manager)
//		assert.NotNil(t, err)
//	}
//}
//
//func TestDialectGetDatastore(t *testing.T) {
//	factory := dsc.NewManagerFactory()
//	configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store.json")
//	manager, err := factory.CreateFromURL(configUrl)
//	assert.Nil(t, err)
//	dialect := dsc.GetDatastoreDialect("aerospike")
//	name, err := dialect.GetCurrentDatastore(manager)
//	assert.Nil(t, err)
//	assert.Equal(t, "test", name)
//}
//
//func TestDialectGetTables(t *testing.T) {
//	factory := dsc.NewManagerFactory()
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store1.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//
//		assert.NotNil(t, manager)
//
//		dialect := dsc.GetDatastoreDialect("aerospike")
//		_, err = dialect.GetTables(manager, "test")
//		assert.Nil(t, err)
//	}
//
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store2.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//		dialect := dsc.GetDatastoreDialect("aerospike")
//		_, err = dialect.GetTables(manager, "test")
//		assert.NotNil(t, err)
//	}
//}
//
//func TestDropTable(t *testing.T) {
//
//	factory := dsc.NewManagerFactory()
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store3.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//		assert.NotNil(t, manager)
//		dialect := dsc.GetDatastoreDialect("aerospike")
//		err = dialect.DropTable(manager, "test10", "abc")
//		assert.Nil(t, err)
//	}
//	{
//		configUrl := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/config/store3.json")
//		manager, err := factory.CreateFromURL(configUrl)
//		assert.Nil(t, err)
//		assert.NotNil(t, manager)
//		dialect := dsc.GetDatastoreDialect("aerospike")
//		err = dialect.DropTable(manager, "test10 abc", "abc")
//		assert.Nil(t, err)
//	}
//}
