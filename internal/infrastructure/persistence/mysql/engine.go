package mysql

import (
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
