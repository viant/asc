package asc

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

const (
	defaultTimeFormat = "yyyy-MM-dd HH:mm:ss z"
	DateFormatKey       = "dateFormat"
	DateLayoutKey       = "dateLayout"
)

type managerFactory struct{}

func (f *managerFactory) Create(config *dsc.Config) (dsc.Manager, error) {
	if !config.Has(DateFormatKey) {
		config.Parameters[DateFormatKey] = defaultTimeFormat
	}

	if ! config.Has(DateLayoutKey) {
		config.Parameters[DateLayoutKey] = toolbox.DateFormatToLayout(config.Get(DateFormatKey))
	}

	var connectionProvider = newConnectionProvider(config)

	manager := &manager{}

	var self dsc.Manager = manager
	super := dsc.NewAbstractManager(config, connectionProvider, self)
	manager.AbstractManager = super
	var err error
	manager.config, err = newConfig(config)
	return self, err
}

func (f managerFactory) CreateFromURL(URL string) (dsc.Manager, error) {
	config, err := dsc.NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return f.Create(config)
}

func newManagerFactory() dsc.ManagerFactory {
	var result dsc.ManagerFactory = &managerFactory{}
	return result
}
