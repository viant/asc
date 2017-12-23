package asc

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type scanner struct {
	query     *dsc.QueryStatement
	columns   []string
	converter toolbox.Converter
	Values    aerospike.BinMap
}

func (s *scanner) Columns() ([]string, error) {
	return s.columns, nil
}

func (s *scanner) Scan(destinations ...interface{}) error {
	var columns, _ = s.Columns()

	if len(destinations) == 1 {
		if toolbox.IsMap(destinations[0]) {
			aMap := toolbox.AsMap(destinations[0])
			for k, v := range s.Values {
				aMap[k] = v
			}
		}
		return nil
	}


	for i, dest := range destinations {
		if dest == nil {
			continue
		}
		if value, found := s.Values[columns[i]]; found {
			err := s.converter.AssignConverted(dest, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func newScanner(query *dsc.QueryStatement, config *dsc.Config, columns []string) *scanner {
	converter := *toolbox.NewColumnConverter(config.GetDateLayout())
	return &scanner{
		query:     query,
		columns:   columns,
		converter: converter,
	}
}
