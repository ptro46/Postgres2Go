package main

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"
)

type Table struct {
	Oid                  string
	Name                 string
	IsVisible            bool
	columns              []*Column
	PrimaryKeyName       string
	PrimaryKeyConstraint string
	foreignKeys          []*ForeignKey
}

func NewTable(oid_ string, name_ string) *Table {
	return &Table{Oid: oid_, Name: name_, IsVisible: false}
}

func (table *Table) generatePrimaryKeyConstraint() {
	s := strings.Replace(table.PrimaryKeyConstraint, "(", "'", -1)
	s = strings.Replace(s, ")", "'", -1)
	re := regexp.MustCompile("PRIMARY KEY '([a-zA-Z0-9_-]+)'")
	matchArray := re.FindSubmatch([]byte(s))
	if len(matchArray) == 2 {
		primaryKeyColumn := string(matchArray[1])
		for _, column := range table.columns {
			if column.Name == primaryKeyColumn {
				column.IsPrimary = true
			}
		}
	}
}

func (table *Table) generateForeignKeysConstraint(logWriter *bufio.Writer) {
	for _, foreignKey := range table.foreignKeys {
		err := foreignKey.parse()
		if err == nil {
			for _, column := range table.columns {
				if column.Name == foreignKey.ColumnName {
					column.IsForeign = true
				}
			}
		} else {
			fmt.Fprintln(logWriter, err)
		}
	}
}
