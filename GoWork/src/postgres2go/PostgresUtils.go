package main

import (
	"bufio"
	"database/sql" // package SQL
	"errors"
	"fmt"
	_ "github.com/lib/pq" // driver Postgres
)

func readTableList(db *sql.DB) []string {
	result := make([]string, 0, 0)

	sqlTableList := "SELECT c.relname as Name FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1"
	rows, err := db.Query(sqlTableList)
	if err != nil {
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return make([]string, 0, 0)
		}
		result = append(result, tableName)
	}
	return result
}

func getTableNameOID(db *sql.DB, tableName string) (*Table, error) {
	sqlTableOID := "SELECT c.oid,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname=$1 AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2"
	rows, err := db.Query(sqlTableOID, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var oid string
		var name string
		err = rows.Scan(&oid, &name)
		if err != nil {
			return nil, err
		}
		return NewTable(oid, name), nil
	}
	return nil, errors.New("No Results")
}

func getForeignKeysList(db *sql.DB, table *Table) error {
	result := make([]*ForeignKey, 0, 0)
	sqlColumns := "SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef FROM pg_catalog.pg_constraint r WHERE r.conrelid=$1 AND r.contype = 'f' ORDER BY 1"
	rows, err := db.Query(sqlColumns, table.Oid)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var conname string
		var condef string
		err = rows.Scan(&conname, &condef)
		if err != nil {
			return err
		}
		fk := NewForeignKey(conname, condef)
		result = append(result, fk)
	}
	table.foreignKeys = result
	return nil
}

func getColumnsList(db *sql.DB, table *Table) error {
	result := make([]*Column, 0, 0)
	sqlColumns := "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull FROM pg_catalog.pg_attribute a WHERE a.attrelid=$1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
	rows, err := db.Query(sqlColumns, table.Oid)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		var formatType string
		var isnullable bool
		err = rows.Scan(&columnName, &formatType, &isnullable)
		if err != nil {
			return err
		}
		column := NewColumn(columnName, formatType, isnullable)
		result = append(result, column)
	}
	table.columns = result
	return nil
}

func getPrimaryKeyConstraint(db *sql.DB, table *Table) error {
	sqlPrimaryKey := "SELECT c2.relname, pg_catalog.pg_get_constraintdef(con.oid, true) FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid=$1 AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname"
	rows, err := db.Query(sqlPrimaryKey, table.Oid)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var relname string
		var constraint string
		err = rows.Scan(&relname, &constraint)
		if err != nil {
			return err
		}
		table.PrimaryKeyName = relname
		table.PrimaryKeyConstraint = constraint
		return nil
	}
	return errors.New("No PrimaryKey")
}

func descTable(logWriter *bufio.Writer, db *sql.DB, tableName string) (*Table, error) {
	table, err := getTableNameOID(db, tableName)
	if err == nil {
		fmt.Fprintf(logWriter, "%s::%s ", table.Oid, table.Name)
		err := getColumnsList(db, table)
		if err != nil {
			return table, err
		}
		fmt.Fprintf(logWriter, " columns::%d ", len(table.columns))

		err = getPrimaryKeyConstraint(db, table)
		if err != nil {
			fmt.Fprintf(logWriter, " NoPrimaryKey ")
		} else {
			fmt.Fprintf(logWriter, " %s::%s ", table.PrimaryKeyName, table.PrimaryKeyConstraint)
		}

		err = getForeignKeysList(db, table)
		if err != nil {
			fmt.Fprintf(logWriter, " NoForeignKeys\n")
		} else {
			fmt.Fprintf(logWriter, " foreignKeys::%d\n", len(table.foreignKeys))
		}

	}
	return table, nil
}
