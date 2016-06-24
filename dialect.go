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

//Package asc - Aerospike datastore dialect
package asc

import (
	"fmt"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

var defaulConnectionTimeout = 500 * time.Millisecond

type dialect struct{ dsc.DatastoreDialect }

func getConnection(config *dsc.Config) (*aerospike.Connection, error) {
	hostPort := config.Get(host) + ":" + config.Get(port)
	connectionTimeoutInMs := defaulConnectionTimeout
	if config.Has(connectionTimeout) {
		timeout := toolbox.AsInt(config.Get(connectionTimeout))
		connectionTimeoutInMs = time.Duration(timeout) * time.Millisecond
	}
	return aerospike.NewConnection(hostPort, connectionTimeoutInMs)
}

func (d dialect) SendAdminCommand(manager dsc.Manager, command string) (map[string]string, error) {
	connection, err := getConnection(manager.Config())
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return aerospike.RequestInfo(connection, command)
}

func (d dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	//result, err := d.SendAdminCommand(manager ,fmt.Sprintf("set-config:context=namespace;id=%v;set=%v;set-delete=true", datastore, table))
	_, err := manager.Execute("DELETE FROM " + table)
	if err != nil {
		return err
	}
	return nil
}

func (d dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	result, err := d.SendAdminCommand(manager, "namespaces")
	if err != nil {
		return nil, err
	}
	if value, found := result["namespaces"]; found {
		return strings.Split(value, ";"), nil
	}
	return nil, fmt.Errorf("Failed to lookup datastores :%v", result)
}

func (d dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get("namespace"), nil
}

func (d dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	command := fmt.Sprintf("sets/%v", datastore)
	result, err := d.SendAdminCommand(manager, command)
	if err != nil {
		return nil, err
	}
	var tables = make([]string, 0)
	if value, found := result[command]; found {
		for _, item := range strings.Split(value, ":") {
			if strings.HasPrefix(item, "set_name") {
				var setName = item[9:len(item)]
				tables = append(tables, setName)
			}
		}
		return tables, nil
	}
	return nil, fmt.Errorf("Failed to get tables %v", result)
}

func (d dialect) CanPersistBatch() bool {
	return true
}

func newDialect() dsc.DatastoreDialect {
	var resut dsc.DatastoreDialect = &dialect{dsc.NewDefaultDialect()}
	return resut
}
