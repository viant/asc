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

//package asc - Aersopike datastore manager
package asc

/*

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

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

const (
	keyColumnName             = "keyColumnName"
	keyColumnNameDefaultValue = "id"
	generationColumnName      = "generationColumnName"
	namespace                 = "namespace"
	connectionTimeout         = "connectionTimeout"
	host                      = "host"
	port                      = "port"
)

type config struct {
	*dsc.Config
	keyColunName         string
	generationColumnName string
	namespace            string
}

type manager struct {
	*dsc.AbstractManager
	aerospikeConfig *config
}

func convertIfNeeded(source interface{}) interface{} {
	if source == nil {
		return nil
	}
	if sourceValue, ok := source.(reflect.Value); ok {
		source = sourceValue.Interface()
	}
	reflectValue := reflect.ValueOf(source)
	if reflectValue.Kind() == reflect.Interface || reflectValue.Kind() == reflect.Ptr {
		if !reflectValue.IsValid() || reflectValue.IsNil() {
			return nil
		}
		reflectValue = reflectValue.Elem()
		source = reflectValue.Interface()
	}

	switch sourceValue := source.(type) {
	case time.Time:
		return sourceValue.String()
	case []byte:
		return sourceValue
	}

	switch reflectValue.Kind() {
	case reflect.Map:
		var newMap = make(map[interface{}]interface{})
		toolbox.ProcessMap(source, func(key, value interface{}) bool {
			newMap[convertIfNeeded(key)] = convertIfNeeded(value)
			return true
		})
		return newMap
	case reflect.Array, reflect.Slice:
		var newSlice = make([]interface{}, 0)
		toolbox.ProcessSlice(source, func(item interface{}) bool {
			newSlice = append(newSlice, convertIfNeeded(item))
			return true
		})
		return newSlice
	case reflect.String, reflect.Bool:
		return toolbox.AsString(source)
	case reflect.Float32, reflect.Float64:
		stringValue := toolbox.AsString(source)
		if strings.Contains(stringValue, ".") {
			return stringValue
		}
		return toolbox.AsInt(stringValue)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		stringValue := toolbox.AsString(source)
		converted, err := strconv.ParseInt(stringValue, 10, 64)
		if err != nil {
			return source
		}
		return int(converted)

	}
	debug.PrintStack()
	panic(fmt.Sprintf("Unsupproted %T %v\n", source, source))
}

func (am *manager) buildUpdateData(statement *dsc.DmlStatement, dmlParameters []interface{}) (key *aerospike.Key, binMap aerospike.BinMap, generation uint32, err error) {
	keyColumnName := am.aerospikeConfig.keyColunName
	namespace := am.aerospikeConfig.namespace
	parameters := toolbox.NewSliceIterator(dmlParameters)

	columnValueMap, err := statement.ColumnValueMap(parameters)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("Failed to prepare update data: [%v] due to %v", dmlParameters, err)
	}
	binMap = make(aerospike.BinMap)
	for key, value := range columnValueMap {
		if value != nil {
			value = convertIfNeeded(value)
		}
		binMap[key] = value
	}

	if len(statement.Criteria) != 1 || statement.Criteria[0].LeftOperand != keyColumnName {
		return nil, nil, 0, fmt.Errorf("Invalid criteria - expected where clause on %v, but had %v", keyColumnName, statement.Criteria)
	}
	keyValues, err := statement.CriteriaValues(parameters)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("Failed to prepare update criteria values: [%v] due to %v", dmlParameters, err)
	}

	key, err = aerospike.NewKey(namespace, statement.Table, convertIfNeeded(keyValues[0]))
	if err != nil {
		return nil, nil, 0, err
	}
	binMap[keyColumnName] = convertIfNeeded(keyValues[0])
	return key, binMap, generation, err
}

func (am *manager) buildInsertData(statement *dsc.DmlStatement, dmlParameters []interface{}) (key *aerospike.Key, binMap aerospike.BinMap, err error) {
	keyColumnName := am.aerospikeConfig.keyColunName
	namespace := am.aerospikeConfig.namespace
	binMap = make(aerospike.BinMap)
	parameters := toolbox.NewSliceIterator(dmlParameters)
	keyValues, err := statement.ColumnValueMap(parameters)
	if err != nil {
		return nil, nil, err
	}
	for k, originalValue := range keyValues {
		if len(k) > 14 {
			return nil, nil, fmt.Errorf("Failed to build insert dataset column name:'%v' to long max 14 characters", k)
		}
		value := convertIfNeeded(originalValue)
		if k == keyColumnName {
			key, err = aerospike.NewKey(namespace, statement.Table, value)
			if err != nil {
				return nil, nil, err
			}
		}
		binMap[k] = value
	}
	if key == nil {
		return nil, nil, fmt.Errorf("Failed to build insert data: key was nil - (should not be tagged as autoincrement")
	}
	return key, binMap, err
}

func (am *manager) deleteAll(client *aerospike.Client, statement *dsc.DmlStatement) (result sql.Result, err error) {
	recordset, err := client.ScanAll(client.DefaultScanPolicy, am.aerospikeConfig.namespace, statement.Table)
	if err != nil {
		return nil, err
	}
	var i = 0
	for record := range recordset.Records {
		writePolicy := aerospike.NewWritePolicy(record.Generation, 0)
		sucessed, err := client.Delete(writePolicy, record.Key)
		if err != nil {
			return nil, err
		}
		if sucessed {
			i++
		}
	}
	return dsc.NewSQLResult(int64(i), 0), nil
}

func (am *manager) deleteSelected(client *aerospike.Client, statement *dsc.DmlStatement, dmlParameters []interface{}) (result sql.Result, err error) {
	parameters := toolbox.NewSliceIterator(dmlParameters)
	keys, err := am.buildKeysForCriteria(&statement.BaseStatement, parameters)
	if err != nil {
		return nil, err
	}
	var i = 0
	for _, key := range keys {
		writePolicy := aerospike.NewWritePolicy(0, 0)
		writePolicy.GenerationPolicy = aerospike.NONE
		sucessed, err := client.Delete(writePolicy, key)
		if err != nil {
			return nil, err
		}
		if sucessed {
			i++
		}
	}
	return dsc.NewSQLResult(int64(i), 0), nil
}

func (am *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	client, err := asClient(connection.Unwrap(clientPointer))
	if err != nil {
		return nil, err
	}
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	var key *aerospike.Key
	var binMap aerospike.BinMap
	var generation uint32
	var writingPolicy = aerospike.NewWritePolicy(0, 0)
	writingPolicy.SendKey = true
	writingPolicy.GenerationPolicy = aerospike.NONE

	switch statement.Type {
	case "INSERT":
		writingPolicy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
		key, binMap, err = am.buildInsertData(statement, sqlParameters)
		if err == nil {
			err = client.Put(writingPolicy, key, binMap)
		}
	case "UPDATE":

		key, binMap, generation, err = am.buildUpdateData(statement, sqlParameters)

		if err == nil {
			writingPolicy.Generation = generation
			if generation > 0 {
				writingPolicy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
			}
			err = client.Put(writingPolicy, key, binMap)
		}
	case "DELETE":
		if len(statement.Criteria) == 0 {
			return am.deleteAll(client, statement)
		}
		return am.deleteSelected(client, statement, sqlParameters)

	}
	if err != nil {
		return nil, err
	}
	return dsc.NewSQLResult(1, 0), nil
}

func (am *manager) scanAll(client *aerospike.Client, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	var err error
	var recordset *aerospike.Recordset
	if statement.AllField {
		recordset, err = client.ScanAll(client.DefaultScanPolicy, am.aerospikeConfig.namespace, statement.Table)
	} else {
		recordset, err = client.ScanAll(client.DefaultScanPolicy, am.aerospikeConfig.namespace, statement.Table, statement.ColumnNames()...)
	}
	if err != nil {
		return err
	}
	defer recordset.Close()
	return am.processRecordset(recordset, statement, readingHandler)
}

func (am *manager) buildKeysForCriteria(statement *dsc.BaseStatement, parameters toolbox.Iterator) ([]*aerospike.Key, error) {
	var result = make([]*aerospike.Key, 0)
	criteria := statement.Criteria[0]
	namespace := am.aerospikeConfig.namespace
	keyColumnName := am.aerospikeConfig.keyColunName
	if criteria.LeftOperand != keyColumnName {
		return nil, fmt.Errorf("Only criteria on key column: '%v' is supproted: %v", keyColumnName, criteria.LeftOperand)
	}
	criteriaValues, err := statement.CriteriaValues(parameters)
	if err != nil {
		return nil, err
	}

	for _, criteriaValue := range criteriaValues {
		criteriaValue = convertIfNeeded(criteriaValue)
		value, err := aerospike.NewKey(namespace, statement.Table, criteriaValue)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (am *manager) readBatch(client *aerospike.Client, statement *dsc.QueryStatement, queryParameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	parameters := toolbox.NewSliceIterator(queryParameters)
	keys, err := am.buildKeysForCriteria(&statement.BaseStatement, parameters)
	if err != nil {
		return err
	}
	var records []*aerospike.Record
	if statement.AllField {
		records, err = client.BatchGet(client.DefaultPolicy, keys)
	} else {
		records, err = client.BatchGet(client.DefaultPolicy, keys, statement.ColumnNames()...)
	}
	if err != nil {
		return err
	}
	err = am.processRecords(records, keys, statement, readingHandler)
	if err != nil {
		return err
	}
	return nil
}

func (am *manager) processRecord(key *aerospike.Key, record *aerospike.Record, aeroSpikeScanner *scanner, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) (toContinue bool, err error) {
	keyColumnName := am.aerospikeConfig.keyColunName
	generationColumnName := am.aerospikeConfig.generationColumnName
	var bins = record.Bins
	if generationColumnName != "" {
		bins[generationColumnName] = record.Generation
	}
	if _, found := bins[keyColumnName]; !found {
		bins[keyColumnName] = key.Value()
	}
	aeroSpikeScanner.Values = bins
	var scanner dsc.Scanner = aeroSpikeScanner
	if err != nil {
		return false, err
	}
	return readingHandler(scanner)
}

func (am *manager) processRecords(records []*aerospike.Record, keys []*aerospike.Key, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	if len(records) == 0 {
		return nil
	}
	for i, record := range records {
		if record != nil {
			columns := toolbox.MapKeysToStringSlice(record.Bins)
			aeroSpikeScanner := newScanner(statement, am.Config(), columns)
			toContinue, err := am.processRecord(keys[i], record, aeroSpikeScanner, readingHandler)
			if err != nil {
				return fmt.Errorf("Failed to fetch full scan data on statement %v, due to\n\t%v", statement.SQL, err)
			}
			if !toContinue {
				return nil
			}
		}
	}
	return nil
}

func (am *manager) processRecordset(recordset *aerospike.Recordset, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	for record := range recordset.Results() {

		if record.Err != nil {
			return fmt.Errorf("Failed to fetch full scan data on statement %v, due to\n\t%v", statement.SQL, record.Err)
		}
		if record.Record != nil {
			columns := toolbox.MapKeysToStringSlice(record.Record.Bins)
			aeroSpikeScanner := newScanner(statement, am.Config(), columns)

			toContinue, err := am.processRecord(record.Record.Key, record.Record, aeroSpikeScanner, readingHandler)
			if err != nil {
				return fmt.Errorf("Failed to fetch full scan data on statement %v, due to\n\t%v", statement.SQL, err)
			}
			if !toContinue {
				return nil
			}
		}

	}
	return nil
}

func (am *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, sql string, args []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	client, err := asClient(connection.Unwrap(clientPointer))
	if err != nil {
		return err
	}
	parser := dsc.NewQueryParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return fmt.Errorf("Failed to parse statement %v, %v", sql, err)
	}
	if statement.Criteria == nil || len(statement.Criteria) == 0 {
		return am.scanAll(client, statement, readingHandler)
	} else if len(statement.Criteria) > 1 {
		return fmt.Errorf("Only single crieria is allowed %v", sql)
	} else {
		return am.readBatch(client, statement, args, readingHandler)
	}
}

func newConfig(iConfig *dsc.Config) *config {
	var keyColumnName = keyColumnNameDefaultValue
	if iConfig.Has(keyColumnName) {
		keyColumnName = iConfig.Get(keyColumnName)
	}
	namespace := iConfig.Get(namespace)
	var generationColumnNameValue string
	if iConfig.Has(generationColumnName) {
		value := iConfig.Get(generationColumnName)
		generationColumnNameValue = value
	}
	return &config{
		Config:               iConfig,
		namespace:            namespace,
		keyColunName:         keyColumnName,
		generationColumnName: generationColumnNameValue,
	}
}
