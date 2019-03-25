package asc

import (
	"database/sql"
	"fmt"
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	pkColumnNameKey          = "keyColumnName"
	readTimeoutMsKey         = "readTimeoutMs"
	connectionTimeoutMsKey   = "connectionTimeoutMs"
	optimizeLargeScanKey     = "optimizeLargeScan"
	scanBaseDirectoryKey     = "scanBaseDirectory"
	pkColumnNameDefaultValue = "id"
	inheritIdFromPKKey       = "inheritIdFromPK"
	generationColumnNameKey  = "generationColumnName"
	excludedColumnsKey       = "excludedColumns"
	batchSizeKey             = "batchSize"
	namespaceKey             = "namespace"
	hostKey                  = "host"
	portKey                  = "port"
)

type config struct {
	*dsc.Config
	keyColumnName        string
	generationColumnName string
	namespace            string
	excludedColumns      []string
	inheritIdFromPK      bool
}

type manager struct {
	*dsc.AbstractManager
	config      *config
	scanPolicy  *aerospike.ScanPolicy
	batchPolicy *aerospike.BatchPolicy
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
	case *time.Time:
		return sourceValue.String()
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
		if toolbox.AsFloat(toolbox.AsInt(source)) == source {
			return toolbox.AsInt(source)
		}
		return toolbox.AsString(source)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		stringValue := toolbox.AsString(source)
		converted, err := strconv.ParseInt(stringValue, 10, 64)
		if err != nil {
			return source
		}
		return int(converted)

	}
	panic(fmt.Sprintf("unsupported %T %v\n", source, source))
}

func (m *manager) buildUpdateData(statement *dsc.DmlStatement, dmlParameters []interface{}) (key *aerospike.Key, binMap aerospike.BinMap, generation uint32, err error) {
	keyColumnName := m.config.keyColumnName
	namespace := m.config.namespace
	parameters := toolbox.NewSliceIterator(dmlParameters)

	columnValueMap, err := statement.ColumnValueMap(parameters)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to prepare update data: [%v] due to %v", dmlParameters, err)
	}
	binMap = make(aerospike.BinMap)
	for key, value := range columnValueMap {
		if value != nil {
			value = convertIfNeeded(value)
		}
		binMap[key] = value
	}

	if len(statement.Criteria) != 1 || statement.Criteria[0].LeftOperand != keyColumnName {
		return nil, nil, 0, fmt.Errorf("invalid criteria - expected where clause on %v, but had %v", keyColumnName, statement.Criteria)
	}
	keyValues, err := statement.CriteriaValues(parameters)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to prepare update criteria values: [%v] due to %v", dmlParameters, err)
	}

	key, err = aerospike.NewKey(namespace, statement.Table, convertIfNeeded(keyValues[0]))
	if err != nil {
		return nil, nil, 0, err
	}
	binMap[keyColumnName] = convertIfNeeded(keyValues[0])
	return key, binMap, generation, err
}

func (m *manager) buildInsertData(statement *dsc.DmlStatement, dmlParameters []interface{}) (key *aerospike.Key, binMap aerospike.BinMap, err error) {

	keyColumnName := m.config.keyColumnName
	namespace := m.config.namespace
	binMap = make(aerospike.BinMap)
	parameters := toolbox.NewSliceIterator(dmlParameters)
	keyValues, err := statement.ColumnValueMap(parameters)
	if err != nil {
		return nil, nil, err
	}
	for k, originalValue := range keyValues {
		if len(k) > 14 {
			return nil, nil, fmt.Errorf("failed to build insert dataset column name:'%v' to long max 14 characters", k)
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
		return nil, nil, fmt.Errorf("failed to build insert data: key was nil - (should not be tagged as autoincrement")
	}
	return key, binMap, err
}

func (m *manager) deleteAll(client *aerospike.Client, statement *dsc.DmlStatement) (result sql.Result, err error) {
	recordset, err := client.ScanAll(client.DefaultScanPolicy, m.config.namespace, statement.Table)
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

func (m *manager) deleteSelected(client *aerospike.Client, statement *dsc.DmlStatement, dmlParameters []interface{}) (result sql.Result, err error) {
	parameters := toolbox.NewSliceIterator(dmlParameters)
	keys, err := m.buildKeysForCriteria(statement.BaseStatement, parameters)
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

func (m *manager) removedEmptyOrExcluded(binMap aerospike.BinMap) {
	for k, v := range binMap {
		if v == nil {
			delete(binMap, k)
		}
	}
	if len(m.config.excludedColumns) == 0 {
		return
	}
	for _, column := range m.config.excludedColumns {
		delete(binMap, column)
	}
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {

	client, err := asClient(connection.Unwrap(clientPointer))
	if err != nil {
		return nil, err
	}
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %v due to %v", sql, err)
	}

	var key *aerospike.Key
	var binMap aerospike.BinMap
	var generation uint32
	var writingPolicy = aerospike.NewWritePolicy(0, 0)
	writingPolicy.SendKey = true
	writingPolicy.GenerationPolicy = aerospike.NONE

	switch statement.Type {
	case "INSERT":
		if m.config.generationColumnName != "" {
			writingPolicy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
		}
		key, binMap, err = m.buildInsertData(statement, sqlParameters)
		m.removedEmptyOrExcluded(binMap)
		if err == nil {
			if len(binMap) > 0 {
				err = client.Put(writingPolicy, key, binMap)
			}
		}
		if err != nil {
			err = fmt.Errorf("failed to insert %v %v, %v", key, binMap, err)
		}
	case "UPDATE":

		key, binMap, generation, err = m.buildUpdateData(statement, sqlParameters)

		if err == nil {
			writingPolicy.Generation = generation
			if generation > 0 {
				writingPolicy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
			}
			m.removedEmptyOrExcluded(binMap)
			if len(binMap) > 0 {
				err = client.Put(writingPolicy, key, binMap)
				if err != nil {
					err = fmt.Errorf("failed to update %v %v, %v", key, binMap, err)
				}
			}
		}
	case "DELETE":
		if len(statement.Criteria) == 0 {
			return m.deleteAll(client, statement)
		}
		return m.deleteSelected(client, statement, sqlParameters)

	}
	if err != nil {
		return nil, err
	}
	return dsc.NewSQLResult(1, 0), nil
}

func normalizeColumnAliases(columns []*dsc.SQLColumn) []string {
	var result = make([]string, 0)
	for _, column := range columns {
		var alias = column.Alias
		if alias == "" {
			alias = column.Name
		}
		result = append(result, alias)
	}
	return result
}

func normalizeQueryColumns(columns []*dsc.SQLColumn) []string {
	var result = make([]string, 0)
	var indexColumns = map[string]bool{}
	for _, column := range columns {
		var name = column.Name
		if column.Expression != "" {
			var expr = column.Expression
			name = strings.TrimSpace(string(expr[strings.LastIndex(expr, "(")+1 : strings.Index(expr, ")")]))
		}
		index := strings.Index(name, ".")
		if index == -1 {
			index = strings.Index(name, ",")
		}
		if index != -1 {
			name = string(name[:index])
		}
		if _, has := indexColumns[name]; has {
			continue
		}
		indexColumns[name] = true
		result = append(result, name)
	}
	return result
}

func (m *manager) scanAll(client *aerospike.Client, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	var err error
	var recordset *aerospike.Recordset

	if m.config.GetBoolean(optimizeLargeScanKey, false) {
		return m.scanAllWithKeys(client, statement, readingHandler, normalizeQueryColumns(statement.Columns)...)
	}

	scanPolicy := client.DefaultScanPolicy
	m.applyPolicySettings(scanPolicy.BasePolicy.GetBasePolicy())

	if statement.AllField {
		recordset, err = client.ScanAll(scanPolicy, m.config.namespace, statement.Table)
	} else {
		recordset, err = client.ScanAll(scanPolicy, m.config.namespace, statement.Table, normalizeQueryColumns(statement.Columns)...)
	}

	if err != nil {
		return err
	}
	defer recordset.Close()
	return m.processRecordset(recordset, statement, readingHandler)
}

func (m *manager) scanAllWithKeys(client *aerospike.Client, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error), binNames ...string) error {
	scanPolicy := m.getScanKeyPolicy()
	baseDirectory := m.config.GetString(scanBaseDirectoryKey, "")
	keyScanner := NewKeyScanner(client, scanPolicy, baseDirectory, m.config.namespace, statement.Table)
	filenames, err := keyScanner.Scan()
	if err != nil {
		return err
	}

	batchPolicy := m.getBatchPolicy()
	var batchSize = m.config.GetInt(batchSizeKey, 256)
	iterator := NewBatchIterator(client, batchPolicy, batchSize, m.config.namespace, statement.Table, filenames, binNames...)
	for iterator.HasNext() {
		var batch = &Batch{}
		err = iterator.Next(&batch)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				break
			}
			return err
		}
		err = m.processRecords(batch.Records, batch.Keys, statement, readingHandler)

		if err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) buildKeysForCriteria(statement *dsc.BaseStatement, parameters toolbox.Iterator) ([]*aerospike.Key, error) {
	var result = make([]*aerospike.Key, 0)
	criteria := statement.Criteria[0]
	namespace := m.config.namespace
	keyColumnName := m.config.keyColumnName
	if criteria.LeftOperand != keyColumnName {
		return nil, fmt.Errorf("only criteria on key column: '%v' is supproted: %v", keyColumnName, criteria.LeftOperand)
	}
	criteriaValues, err := statement.CriteriaValues(parameters)
	if err != nil {
		return nil, err
	}

	for _, criteriaValue := range criteriaValues {
		criteriaValue = convertIfNeeded(criteriaValue)
		if criteriaValue == nil {
			continue
		}

		value, err := aerospike.NewKey(namespace, statement.Table, criteriaValue)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (m *manager) readBatch(client *aerospike.Client, statement *dsc.QueryStatement, queryParameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	parameters := toolbox.NewSliceIterator(queryParameters)
	keys, err := m.buildKeysForCriteria(statement.BaseStatement, parameters)
	if err != nil {
		return err
	}
	var batchPolicy = m.getBatchPolicy()
	var records []*aerospike.Record

	maxRetries := m.Config().GetInt("maxRetries", 2)
	if maxRetries == 0 {
		maxRetries = 1
	}
	for i := 0; i < maxRetries; i++ {
		if statement.AllField {
			records, err = client.BatchGet(batchPolicy, keys)
		} else {
			records, err = client.BatchGet(batchPolicy, keys, normalizeQueryColumns(statement.Columns)...)
		}
		if err != nil {
			return err
		}
		break
	}
	err = m.processRecords(records, keys, statement, readingHandler)
	if err != nil {
		return err
	}
	return nil
}

func (m *manager) processRecord(key *aerospike.Key, record *aerospike.Record, scanner *dsc.SQLScanner, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) (toContinue bool, err error) {
	generationColumnName := m.config.generationColumnName
	var bins = record.Bins
	if generationColumnName != "" {
		bins[generationColumnName] = record.Generation
	}

	if m.config.inheritIdFromPK {
		keyColumnName := m.config.keyColumnName
		if _, found := bins[keyColumnName]; !found && key != nil && key.Value() != nil {
			bins[keyColumnName] = key.Value().GetObject()
		}
	}
	scanner.Values = bins
	if err != nil {
		return false, err
	}
	return readingHandler(scanner)
}

func hasAliasOrExpression(columns []*dsc.SQLColumn) bool {
	for _, column := range columns {
		if column.Alias != "" {
			return true
		}
		if column.Expression != "" {
			return true
		}
	}
	return false
}

func (m *manager) processRecords(records []*aerospike.Record, keys []*aerospike.Key, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	if len(records) == 0 {
		return nil
	}

	hasAliasOrExpression := hasAliasOrExpression(statement.Columns)
	var columns = normalizeColumnAliases(statement.Columns)
	var err error

	for i, rawRecord := range records {

		if rawRecord != nil {
			if hasAliasOrExpression {
				rawRecord.Bins, columns, err = m.normalizeRecord(statement, rawRecord.Bins)
				if err != nil {
					return err
				}
			}

			scanner := dsc.NewSQLScanner(statement, m.Config(), columns)
			var key = keys[i]
			if rawRecord.Key != nil {
				key = rawRecord.Key
			}
			toContinue, err := m.processRecord(key, rawRecord, scanner, readingHandler)
			if err != nil {
				return fmt.Errorf("failed to read data %v, due to\n\t%v", statement.SQL, err)
			}
			if !toContinue {
				return nil
			}
		}
	}
	return nil
}

func (m *manager) registerUDF(record data.Map) {
	record.Put("TO_JSON_STRING", AsJSON)
	record.Put("ARRAY", AsArray)
	record.Put("TIMESTAMP", AsTimestamp(m.Config()))
}

func (m *manager) normalizeRecord(statement *dsc.QueryStatement, record map[string]interface{}) (map[string]interface{}, []string, error) {
	var columns = make([]string, 0)
	recordMap := data.Map(record)
	var normalizedRecord = make(map[string]interface{}, len(record))
	m.registerUDF(recordMap)
	for _, column := range statement.Columns {
		var alias = column.Alias
		if alias == "" {
			alias = column.Name
		}

		if column.Expression != "" {
			normalizedRecord[alias] = recordMap.Expand("$" + column.Expression)
		} else if column.Alias != "" {
			normalizedRecord[alias], _ = recordMap.GetValue(column.Name)
		} else {
			normalizedRecord[alias] = record[column.Name]
		}
		columns = append(columns, alias)
	}

	return normalizedRecord, columns, nil
}

func (m *manager) processRecordset(recordset *aerospike.Recordset, statement *dsc.QueryStatement, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	var records = recordset.Records
	var errors = recordset.Errors
	var rawRecord *aerospike.Record
	hasAliasOrExpression := hasAliasOrExpression(statement.Columns)
	var columns = normalizeColumnAliases(statement.Columns)
	var err error

	for {
		if !recordset.IsActive() {
			return nil
		}
		select {
		case rawRecord = <-records:
			if rawRecord != nil {
				var aMap map[string]interface{}
				if len(rawRecord.Bins) == 0 {
					aMap = make(map[string]interface{})
				} else {
					aMap = map[string]interface{}(rawRecord.Bins)
				}
				if hasAliasOrExpression {
					rawRecord.Bins, columns, err = m.normalizeRecord(statement, aMap)
					if err != nil {
						return err
					}
				}
				scanner := dsc.NewSQLScanner(statement, m.Config(), columns)
				toContinue, err := m.processRecord(rawRecord.Key, rawRecord, scanner, readingHandler)
				if err != nil {
					return fmt.Errorf("failed to fetch full scan data on statement %v, due to\n\t%v", statement.SQL, err)
				}
				if !toContinue {
					return nil
				}
			}
		case err := <-errors:
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, sql string, args []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	client, err := asClient(connection.Unwrap(clientPointer))
	if err != nil {
		return err
	}

	parser := dsc.NewQueryParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return fmt.Errorf("failed to parse statement %v, %v", sql, err)
	}
	if statement.Criteria == nil || len(statement.Criteria) == 0 {
		return m.scanAll(client, statement, readingHandler)
	} else if len(statement.Criteria) > 1 {
		return fmt.Errorf("only single crieria is allowed %v", sql)
	} else {
		return m.readBatch(client, statement, args, readingHandler)
	}
}

func (m *manager) applyPolicySettings(policy *aerospike.BasePolicy) {
	policy.MaxRetries = m.Config().GetInt("maxRetries", 3)
	policy.SleepBetweenRetries = m.Config().GetDuration("sleepBetweenRetriesMs", time.Millisecond, 100*time.Millisecond)
	policy.SleepMultiplier = m.Config().GetFloat("sleepMultiplier", 1.2)
	policy.SocketTimeout = m.Config().GetDuration("socketTimeout", time.Millisecond, 120000*time.Millisecond)
}

func (m *manager) getScanKeyPolicy() *aerospike.ScanPolicy {
	if m.scanPolicy != nil {
		return m.scanPolicy
	}
	result := aerospike.NewScanPolicy()
	result.SocketTimeout = m.Config().GetDuration("serverSocketTimeout", time.Millisecond, 30000*time.Millisecond)
	result.IncludeBinData = false
	//Testing only option
	scanPercentage := m.Config().GetInt("scanPct", 0)
	if scanPercentage > 0 {
		result.ScanPercent = scanPercentage
	}
	m.applyPolicySettings(result.BasePolicy.GetBasePolicy())
	m.scanPolicy = result
	return result
}

func (m *manager) getBatchPolicy() *aerospike.BatchPolicy {
	if m.batchPolicy != nil {
		return m.batchPolicy
	}
	result := aerospike.NewBatchPolicy()
	m.applyPolicySettings(&result.BasePolicy)
	result.ConcurrentNodes = m.Config().GetInt("concurrentNodes", 3)
	m.batchPolicy = result
	return result
}

func newConfig(conf *dsc.Config) (*config, error) {
	namespace := conf.Get(namespaceKey)
	if namespace == "" {
		return nil, fmt.Errorf("namespaceKey was empty")
	}
	var keyColumnName = conf.GetString(pkColumnNameKey, pkColumnNameDefaultValue)
	var generationColumnNameValue = conf.GetString(generationColumnNameKey, "")
	var inheritIdFromPK = conf.GetBoolean(inheritIdFromPKKey, true)
	var excluded []string
	if conf.Has(excludedColumnsKey) {
		excluded = strings.Split(conf.Get(excludedColumnsKey), ",")
	}

	return &config{
		Config:               conf,
		namespace:            namespace,
		keyColumnName:        keyColumnName,
		excludedColumns:      excluded,
		generationColumnName: generationColumnNameValue,
		inheritIdFromPK:      inheritIdFromPK,
	}, nil
}
