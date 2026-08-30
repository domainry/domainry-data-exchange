package persistence

import (
	"fmt"

	"github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/base"
	mysqlengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/mysql"
	postgresengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/postgres"
	sqliteengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/sqlite"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormdriver "github.com/domainry/domainry-orm/driver"
)

type Engine interface {
	ormdriver.Profile
	Dialect() ormdialect.Dialect
	HistoricalSchema() base.HistoricalSchema
}

var engineFactories = map[ormdialect.Name]func() Engine{
	ormdialect.SQLite:   func() Engine { return sqliteengine.NewEngine() },
	ormdialect.MySQL:    func() Engine { return mysqlengine.NewEngine() },
	ormdialect.Postgres: func() Engine { return postgresengine.NewEngine() },
}

func NewEngine(driver string) (Engine, error) {
	dialect, err := ormdialect.Parse(driver)
	if err != nil {
		return nil, fmt.Errorf("Data Exchange database driver %q is unsupported: %w", driver, err)
	}
	factory := engineFactories[dialect.Name()]
	if factory == nil {
		return nil, fmt.Errorf("Data Exchange database driver %q is unsupported", driver)
	}
	return factory(), nil
}
