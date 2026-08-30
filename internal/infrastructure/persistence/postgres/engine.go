package postgres

import (
	"github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/base"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormpostgres "github.com/domainry/domainry-orm/postgres"
)

type Engine struct {
	ormpostgres.Profile
	dialect ormdialect.Dialect
}

func NewEngine() Engine {
	dialect, _ := ormdialect.New(ormdialect.Postgres)
	return Engine{Profile: ormpostgres.NewProfile(), dialect: dialect}
}

func (engine Engine) Dialect() ormdialect.Dialect { return engine.dialect }
func (Engine) HistoricalSchema() base.HistoricalSchema {
	return base.HistoricalSchema{TextType: "TEXT", KeyType: "TEXT", LargeType: "TEXT", QualifySchema: true}
}
