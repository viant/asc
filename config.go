package asc

import (
	"fmt"
	"github.com/viant/dsc"
	"strings"
	"time"
)

const (
	pkColumnKey         = "keyColumn"
	pkColumnNameKey     = "keyColumnName"
	inheritIdFromPKKey  = "inheritIdFromPK"
	generationColumnKey = "generationColumn"
	excludedColumnsKey  = "excludedColumns"

	serverSocketTimeoutKey   = "serverSocketTimeout"
	connectionTimeoutMsKey   = "connectionTimeoutMs"
	timeoutMsKey             = "timeoutMs"
	maxRetriesKey            = "maxRetries"
	sleepBetweenRetriesMsKey = "sleepBetweenRetriesMs"

	namespaceKey = "namespace"
	hostKey      = "host"
	portKey      = "port"
	scanPctKey   = "scanPct"
	batchSizeKey = "batchSize"

	optimizeLargeScanKey = "optimizeLargeScan"
	scanBaseDirectoryKey = "scanBaseDirectory"
)

type config struct {
	*dsc.Config
	keyColumn           string
	connectionTimeout   time.Duration
	timeout             time.Duration
	serverSocketTimeout time.Duration
	scanPct             int
	maxRetries          int
	sleepBetweenRetries time.Duration
	generationColumn    string
	namespace           string
	excludedColumns     []string
	inheritIdFromPK     bool
	batchSize           int
	optimizeLargeScan   bool
	scanBaseDirectory   string
}

func (m *config) getKeyColumn(table string) string {
	if keyColumn := m.GetString(table+"."+pkColumnKey, ""); keyColumn != "" {
		return keyColumn
	}
	return m.keyColumn
}

func newConfig(conf *dsc.Config) (*config, error) {
	result := &config{
		Config: conf,

		scanPct:             conf.GetInt(scanPctKey, 100),
		serverSocketTimeout: conf.GetDuration(serverSocketTimeoutKey, time.Millisecond, 30000*time.Millisecond),
		connectionTimeout:   conf.GetDuration(connectionTimeoutMsKey, time.Millisecond, 5000*time.Millisecond),
		timeout:             conf.GetDuration(timeoutMsKey, time.Millisecond, 5000*time.Millisecond),
		sleepBetweenRetries: conf.GetDuration(sleepBetweenRetriesMsKey, time.Millisecond, 100*time.Millisecond),
		maxRetries:          conf.GetInt(maxRetriesKey, 3),
		namespace:           conf.Get(namespaceKey),
		generationColumn:    conf.GetString(generationColumnKey, ""),
		keyColumn:           conf.GetString(pkColumnKey, ""),
		inheritIdFromPK:     conf.GetBoolean(inheritIdFromPKKey, true),
		excludedColumns:     strings.Split(conf.Get(excludedColumnsKey), ","),
		batchSize:           conf.GetInt(batchSizeKey, 256),
		optimizeLargeScan:   conf.GetBoolean(optimizeLargeScanKey, false),
		scanBaseDirectory:   conf.GetString(scanBaseDirectoryKey, ""),
	}
	if result.namespace == "" {
		return nil, fmt.Errorf("namespaceKey was empty")
	}
	if result.keyColumn = conf.GetString(pkColumnKey, ""); result.keyColumn == "" {
		result.keyColumn = conf.GetString(pkColumnNameKey, "id")
	}
	return result, nil
}
