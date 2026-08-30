package mysql

import (
	"github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/base"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormmysql "github.com/domainry/domainry-orm/mysql"
)

type Engine struct {
	ormmysql.Profile
	dialect ormdialect.Dialect
}

func NewEngine() Engine {
	dialect, _ := ormdialect.New(ormdialect.MySQL)
	return Engine{Profile: ormmysql.NewProfile(), dialect: dialect}
}

func (engine Engine) Dialect() ormdialect.Dialect { return engine.dialect }
func (Engine) HistoricalSchema() base.HistoricalSchema {
	return base.HistoricalSchema{TextType: "VARCHAR(512)", KeyType: "VARCHAR(191)", LargeType: "LONGTEXT", QualifySchema: true}
}
