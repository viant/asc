package asc

import (
	"fmt"
	"reflect"

	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

var clientPointer = (*aerospike.Client)(nil)

func asClient(wrapped interface{}) (*aerospike.Client, error) {
	if result, ok := wrapped.(*aerospike.Client); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf("Failed cast as *aerospike.Client: was %v !", wrappedType.Type())
}

type connection struct {
	*dsc.AbstractConnection
	client *aerospike.Client
}

func (c *connection) CloseNow() error {
	client := c.client
	client.Close()
	return nil
}

func (c *connection) Unwrap(target interface{}) interface{} {
	if target == clientPointer {
		return c.client
	}
	panic(fmt.Sprintf("Unsupported target type %v", target))
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (p *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := p.ConnectionProvider.Config()
	client, err := aerospike.NewClient(config.Get("host"), toolbox.AsInt(config.Get("port")))
	if err != nil {
		return nil, err
	}

	var aerospikeConnection = &connection{client: client}
	var connection dsc.Connection = aerospikeConnection
	var super = dsc.NewAbstractConnection(config, p.ConnectionProvider.ConnectionPool(), connection)
	aerospikeConnection.AbstractConnection = super
	return connection, nil
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}
