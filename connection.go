/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

//Package asc - Aerospike connection
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
	dsc.AbstractConnection
	client *aerospike.Client
}

func (c *connection) CloseNow() error {
	client := c.client
	client.Close()
	return nil
}

func (c *connection) Begin() error {
	return nil
}

func (c *connection) Unwrap(target interface{}) interface{} {
	if target == clientPointer {
		return c.client
	}
	panic(fmt.Sprintf("Unsupported target type %v", target))
}

func (c *connection) Commit() error {
	return nil
}

func (c *connection) Rollback() error {
	return nil
}

type connectionProvider struct {
	dsc.AbstractConnectionProvider
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
