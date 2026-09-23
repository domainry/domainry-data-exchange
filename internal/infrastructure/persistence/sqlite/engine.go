package sqlite

import (
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormsqlite "github.com/domainry/domainry-orm/sqlite"
)

type Engine struct {
	ormsqlite.Profile
	dialect ormdialect.Dialect
}

func NewEngine() Engine {
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	return Engine{Profile: ormsqlite.NewProfile(), dialect: dialect}
}

func (engine Engine) Dialect() ormdialect.Dialect { return engine.dialect }
