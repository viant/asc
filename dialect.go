package asc

import (
	"fmt"
	"strings"
	"time"

	"errors"
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

var defaulConnectionTimeout = 500 * time.Millisecond

type dialect struct{ dsc.DatastoreDialect }

func getConnection(config *dsc.Config) (*aerospike.Connection, error) {
	if !config.Has(hostKey) || !config.Has(portKey) {
		return nil, errors.New("Port or hostKey are not poresent")
	}
	hostPort := config.Get(hostKey) + ":" + config.Get(portKey)
	connectionTimeoutInMs := defaulConnectionTimeout

	if config.Has(connectionTimeoutMsKey) {
		timeout := toolbox.AsInt(config.Get(connectionTimeoutMsKey))
		connectionTimeoutInMs = time.Duration(timeout) * time.Millisecond
	}
	return aerospike.NewConnection(hostPort, connectionTimeoutInMs)
}

//GetKeyName returns a name of column name that is a key, or coma separated list if complex key

func (d *dialect) GetKeyName(manager dsc.Manager, datastore, table string) string {
	config := manager.Config()
	var keyName = pkColumnNameDefaultValue
	if config.Has(pkColumnNameKey) {
		keyName = config.Get(pkColumnNameKey)
	}
	return keyName
}

func (d *dialect) SendAdminCommand(manager dsc.Manager, command string) (map[string]string, error) {
	connection, err := getConnection(manager.Config())
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return aerospike.RequestInfo(connection, command)
}

func (d *dialect) GetColumns(manager dsc.Manager, datastore, table string) ([]dsc.Column, error) {
	var result = make([]dsc.Column, 0)
	command := fmt.Sprintf("bins/%v", datastore)
	response, err := d.SendAdminCommand(manager, command)

	if err != nil {
		return []dsc.Column{}, nil
	}
	//
	if encodedBins, ok := response[command]; ok {
		encodedFragments := strings.Split(encodedBins, ",")

		for _, fragment := range encodedFragments {
			if strings.HasPrefix(fragment, "bin_names") {
				var binCount = toolbox.AsInt(string(fragment[10:]))
				if binCount > 0 {
					for j := 0; j < binCount; j++ {
						var column = dsc.NewSimpleColumn(encodedFragments[len(encodedFragments)-(j+1)], "")
						result = append(result, column)
					}
					break
				}
			}
		}
	}
	return result, nil
}

func (d *dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	_, err := manager.Execute("DELETE FROM " + table)
	if err != nil {
		return err
	}
	return nil
}

func (d *dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	result, err := d.SendAdminCommand(manager, namespaceKey)
	if err != nil {
		return nil, err
	}
	if value, found := result[namespaceKey]; found {
		return strings.Split(value, ";"), nil
	}
	return nil, fmt.Errorf("failed to lookup datastores :%v", result)
}

func (d *dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get(namespaceKey), nil
}

func (d *dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	command := fmt.Sprintf("sets/%v", datastore)
	result, err := d.SendAdminCommand(manager, command)
	if err != nil {
		return nil, err
	}
	var tables = make([]string, 0)
	if value, found := result[command]; found {
		for _, item := range strings.Split(value, ":") {
			if strings.HasPrefix(item, "set_name") {
				var setName = item[9:]
				tables = append(tables, setName)
			} else if strings.HasPrefix(item, "set=") {
				var setName = item[4:]
				tables = append(tables, setName)
			}
		}
		return tables, nil
	}
	return nil, fmt.Errorf("failed to get tables %v", result)
}

func (d *dialect) CanPersistBatch() bool {
	return false
}

func newDialect() dsc.DatastoreDialect {
	var resut dsc.DatastoreDialect = &dialect{dsc.NewDefaultDialect()}
	return resut
}
