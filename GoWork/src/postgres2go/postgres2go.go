package main

import (
	"bufio"
	"bytes"
	"database/sql" // package SQL
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq" // driver Postgres
	"io/ioutil"
	"os"
	"strings"
)

const (
	STATUS_OK       = 0
	STATUS_WARNING  = 1
	STATUS_CRITICAL = 2
	STATUS_UNKNOW   = 3
)

func main() {
	message := "UNKNOW - "
	status := STATUS_UNKNOW

	workingDirectory := "./"
	postgresConfigFullFileName := "./" + "postgres-to-go.config"
	logFullFileName := workingDirectory + "/postgres-to-go.log"
	logFileHandle, err := os.Create(logFullFileName)
	if err != nil {
		fmt.Println(err)
		message = fmt.Sprintf("CRITICAL - can not create log file %s", logFullFileName)
		status = STATUS_CRITICAL
		fmt.Printf("%s\n", message)
		os.Exit(status)
	}
	defer logFileHandle.Close()
	logWriter := bufio.NewWriter(logFileHandle) // *bufio.Writer

	dbinfo, err := readFile(postgresConfigFullFileName)
	if err == nil {
		postgresToGoConfig := new(PostgresToGoConfig)
		errUnmarshal := json.Unmarshal([]byte(dbinfo), postgresToGoConfig)
		if errUnmarshal == nil {
			db, err := sql.Open("postgres", postgresToGoConfig.dbPostgresConnectString())
			if err == nil {
				err = db.Ping()
				if err == nil {
					fmt.Fprintln(logWriter, "Connected to "+postgresToGoConfig.Db)
					fmt.Println("Connected to " + postgresToGoConfig.Db)
					defer db.Close()
					
					if err == nil {
						fmt.Fprintln(logWriter, "Describe tables")
						fmt.Println("Describe tables")
						tableList := readTableList(db)
						tables := make([]*Table, 0, 0)
						for _, tableName := range tableList {
							table, err := descTable(logWriter, db, tableName)
							if err != nil {
								fmt.Fprintln(logWriter, err)
								fmt.Println(err)
							}
							if nil != table {
								tables = append(tables, table)
							}
						}
						fmt.Fprintln(logWriter, "Generate Primary Keys")
						fmt.Println("Generate Primary Keys")
						for _, table := range tables {
							table.generatePrimaryKeyConstraint()
						}

						fmt.Fprintln(logWriter, "Generate Foreign Keys")
						fmt.Println("Generate Foreign Keys")
						for _, table := range tables {
							table.generateForeignKeysConstraint(logWriter)
						}

						for _, table := range tables {
							fmt.Printf("Table:%s --> %s\n", table.Name, snakeToCamel(table.Name))
							fmt.Printf("\tJsonDataType ")
							err := generateGoJsonMapping( table )
							if err == nil {
								fmt.Printf("Done\n")
							} else {
								fmt.Printf("%+v\n",err)
							}

							fmt.Printf("\tEntity ")
							err = generateGoEntity( table )
							if err == nil {
								fmt.Printf("Done\n")
							} else {
								fmt.Printf("%+v\n",err)
							}

							fmt.Printf("\tDAO ")
							err = generateGoEntityDAO( table )
							if err == nil {
								fmt.Printf("Done\n")
							} else {
								fmt.Printf("%+v\n",err)
							}
						}

					} else {
						message = fmt.Sprintf("CRITICAL - can not connect to postgres database %s", err.Error())
						status = STATUS_CRITICAL
					}
				} else {
					message = fmt.Sprintf("CRITICAL - can not connect to postgres database %s", err.Error())
					status = STATUS_CRITICAL
				}
			} else {
				message = fmt.Sprintf("CRITICAL - can not parse json config file %s", errUnmarshal.Error())
				status = STATUS_CRITICAL
			}
		} else {
			message = fmt.Sprintf("CRITICAL - can not read postgres config file : %s", postgresConfigFullFileName)
			status = STATUS_CRITICAL
		}
		fmt.Fprintf(logWriter, "%s\n", message)
		fmt.Printf("%s\n", message)
		os.Exit(status)
	}
}

func postgresToGoType(postgresType string) string {
	if postgresType == "text" {
		return "string"
	} else if postgresType == "integer" {
		return "int"
	} else if postgresType == "bigint" {
		return "int64"
	} else if postgresType == "double precision" {
		return "float64"
	}
	return "UNKNOW : " + postgresType
}

func postgresToFmtType(postgresType string) string {
	if postgresType == "text" {
		return "%s"
	} else if postgresType == "integer" {
		return "%d"
	} else if postgresType == "bigint" {
		return "%d"
	} else if postgresType == "double precision" {
		return "%f"
	}
	return "UNKNOW : " + postgresType
}

func generateGoJsonMapping(table *Table) error {
	entityName := snakeToCamel(table.Name) + "Json"
	entityFileName := "./outputs/" + entityName + ".go"
	entityHandle, err := os.Create(entityFileName)
	if err != nil {
		return err
	}
	defer entityHandle.Close()
	entityWriter := bufio.NewWriter(entityHandle) // *bufio.Writer
	fmt.Fprintf(entityWriter, "package main\n\n")
	//	fmt.Fprintf(entityWriter, "import (\n\t\"fmt\"\n)\n")
	fmt.Fprintf(entityWriter, "type %s struct {\n",entityName)

	maxNameWidth := 0
	maxTypeWidth := 0
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		if len(camelName) > maxNameWidth {
			maxNameWidth = len(camelName)
		}
		if len(column.Type) > maxTypeWidth {
			maxTypeWidth = len(column.Type)
		}
	}
	maxNameWidth = maxNameWidth + 10 
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		goType := postgresToGoType(column.Type)		
		camelFirstLowName := strings.ToLower(camelName[:1]) + camelName[1:]
		fmt.Fprintf(entityWriter, "\t%-*s\t%-*s\t`json:\"%s,omitempty\"`\n", maxNameWidth, camelName, maxTypeWidth, goType, camelFirstLowName)
	}
	fmt.Fprintf(entityWriter, "}\n\n")

	entityWriter.Flush()
	return nil
}

func generateGoEntity(table *Table) error {
	entityName := snakeToCamel(table.Name)
	entityFileName := "./outputs/" + entityName + ".go"
	entityHandle, err := os.Create(entityFileName)
	if err != nil {
		return err
	}
	defer entityHandle.Close()
	entityWriter := bufio.NewWriter(entityHandle) // *bufio.Writer
	fmt.Fprintf(entityWriter, "package main\n\n")
	fmt.Fprintf(entityWriter, "import (\n\t\"fmt\"\n)\n")
	fmt.Fprintf(entityWriter, "type %s struct {\n",entityName)

	maxNameWidth := 0
	maxTypeWidth := 0
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		if len(camelName) > maxNameWidth {
			maxNameWidth = len(camelName)
		}
		if len(column.Type) > maxTypeWidth {
			maxTypeWidth = len(column.Type)
		}
	}
	maxNameWidth = maxNameWidth + 10 
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		goType := postgresToGoType(column.Type)		
		fmt.Fprintf(entityWriter, "\t%-*s\t%-*s\n", maxNameWidth, camelName, maxTypeWidth, goType)
	}
	fmt.Fprintf(entityWriter, "}\n\n")

	fmt.Fprintf(entityWriter, "func New%s(",entityName)
	waitSemilicon := false
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		goType := postgresToGoType(column.Type)
		camelFirstLowName := strings.ToLower(camelName[:1]) + camelName[1:]
		if waitSemilicon == false {
			fmt.Fprintf(entityWriter, "%s %s", camelFirstLowName, goType)
		} else {
			fmt.Fprintf(entityWriter, ", %s %s", camelFirstLowName, goType)
		}
		waitSemilicon = true
	}
	fmt.Fprintf(entityWriter, ") *%s {\n",entityName)
	fmt.Fprintf(entityWriter, "\treturn &%s{\n",entityName)
	for i, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		camelFirstLowName := strings.ToLower(camelName[:1]) + camelName[1:]
		if i < len(table.columns) - 1 {
			nameToPrint := camelName + ":"
			nameFirstLowToPrint := camelFirstLowName + ","
			fmt.Fprintf(entityWriter, "\t\t%-*s\t%-*s\n", maxNameWidth, nameToPrint, maxNameWidth, nameFirstLowToPrint)
		} else {
			nameToPrint := camelName + ":"
			nameFirstLowToPrint := camelFirstLowName + "}"
			fmt.Fprintf(entityWriter, "\t\t%-*s\t%-*s\n", maxNameWidth, nameToPrint, maxNameWidth, nameFirstLowToPrint)
		}
	}	
	fmt.Fprintf(entityWriter, "}\n\n")

	fmt.Fprintf(entityWriter, "func (d *%s) String() string {\n",entityName)
	fmt.Fprintf(entityWriter, "\treturn fmt.Sprintf(\"%s",entityName)
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		fmtType := postgresToFmtType(column.Type)
		fmt.Fprintf(entityWriter, " %s(%s)",camelName, fmtType)		
	}
	fmt.Fprintf(entityWriter, ")\"")
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		fmt.Fprintf(entityWriter, ", d.%s",camelName)		
	}
	fmt.Fprintf(entityWriter, ")\n")
	fmt.Fprintf(entityWriter, "}\n\n")
	
	entityWriter.Flush()
	return nil
}

func generateGoEntityDAO(table *Table) error {
	var bufferVars bytes.Buffer
	var bufferScan bytes.Buffer
	var bufferNew bytes.Buffer
	entityName := snakeToCamel(table.Name)
	entityFileName := "./outputs/" + entityName + "DAO.go"
	entityHandle, err := os.Create(entityFileName)
	if err != nil {
		return err
	}
	defer entityHandle.Close()
	entityWriter := bufio.NewWriter(entityHandle) // *bufio.Writer
	fmt.Fprintf(entityWriter, "package main\n\n")
	fmt.Fprintf(entityWriter, "import (\n\t\"database/sql\"\n")
	fmt.Fprintf(entityWriter, "\t_ \"github.com/lib/pq\"\n)\n\n")

	waitForSemilicon := false
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		goType := postgresToGoType(column.Type)
		bufferVars.WriteString(fmt.Sprintf("\tvar %s %s\n", camelName, goType))
		if waitForSemilicon == true {
			bufferScan.WriteString(fmt.Sprintf(",&%s", camelName))
			bufferNew.WriteString(fmt.Sprintf(",%s", camelName))
		} else {
			bufferScan.WriteString(fmt.Sprintf("&%s", camelName))
			bufferNew.WriteString(fmt.Sprintf("%s", camelName))
		}
		waitForSemilicon = true
	}

	fmt.Fprintf(entityWriter, "func rowResultSetTo%s(row *sql.Row) (*%s, error) {\n",entityName, entityName)
	fmt.Fprintf(entityWriter, "\tvar err error\n")
	fmt.Fprintf(entityWriter, "%s\n",bufferVars.String())
	fmt.Fprintf(entityWriter, "\terr = row.Scan(%s)\n",bufferScan.String())
	fmt.Fprintf(entityWriter, "\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t}\n")
	fmt.Fprintf(entityWriter, "\treturn New%s(%s),nil\n",entityName, bufferNew.String())
	fmt.Fprintf(entityWriter, "}\n\n")

	fmt.Fprintf(entityWriter, "func rowsNoFetchResultSetTo%s(rows *sql.Rows) (*%s, error) {\n",entityName, entityName)
	fmt.Fprintf(entityWriter, "\tvar err error\n")
	fmt.Fprintf(entityWriter, "%s\n",bufferVars.String())
	fmt.Fprintf(entityWriter, "\terr = rows.Scan(%s)\n",bufferScan.String())
	fmt.Fprintf(entityWriter, "\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t}\n")
	fmt.Fprintf(entityWriter, "\treturn New%s(%s),nil\n",entityName, bufferNew.String())
	fmt.Fprintf(entityWriter, "}\n\n")

	fmt.Fprintf(entityWriter, "func rowsResultSetTo%s(rows *sql.Rows) (*%s, error) {\n",entityName, entityName)
	fmt.Fprintf(entityWriter, "\tvar err error\n")
	fmt.Fprintf(entityWriter, "\tif rows.Next() {\n")
	fmt.Fprintf(entityWriter, "\t%s\n",bufferVars.String())
	fmt.Fprintf(entityWriter, "\t\terr = rows.Scan(%s)\n",bufferScan.String())
	fmt.Fprintf(entityWriter, "\t\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t\t}\n")
	fmt.Fprintf(entityWriter, "\t\treturn New%s(%s),nil\n",entityName, bufferNew.String())
	fmt.Fprintf(entityWriter, "\t}\n")
	fmt.Fprintf(entityWriter, "\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "}\n\n")

	camelFirstLowEntityName := strings.ToLower(entityName[:1]) + entityName[1:]
	fmt.Fprintf(entityWriter, "func load%sById(db *sql.DB, id int64) (*%s, error) {\n", entityName, entityName)
	fmt.Fprintf(entityWriter, "\trows, err := db.Query(\"select ")
	waitForSemilicon = false
	for _, column := range table.columns {
		if waitForSemilicon == true {
			fmt.Fprintf(entityWriter, ",%s",column.Name)
		} else {
			fmt.Fprintf(entityWriter, "%s",column.Name)
		}
		waitForSemilicon = true
	}	
	fmt.Fprintf(entityWriter, " from %s where id=$1\",id)\n", table.Name)
	fmt.Fprintf(entityWriter, "\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t}\n\n")
	fmt.Fprintf(entityWriter, "\t%s, err := rowsResultSetTo%s(rows)\n",camelFirstLowEntityName, entityName)
	fmt.Fprintf(entityWriter, "\tdefer rows.Close()\n")
	fmt.Fprintf(entityWriter, "\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t}\n")
	fmt.Fprintf(entityWriter, "\treturn %s, nil\n",camelFirstLowEntityName)
	fmt.Fprintf(entityWriter, "}\n\n")

	waitForSemilicon = false
	fmt.Fprintf(entityWriter, "func create%s(db *sql.DB, ",entityName)
	for _, column := range table.columns {
		if column.IsPrimary == false {
			camelName := snakeToCamel(column.Name)
			goType := postgresToGoType(column.Type)
			if waitForSemilicon == true {
				fmt.Fprintf(entityWriter, ",%s %s",camelName, goType)
			} else {
				fmt.Fprintf(entityWriter, "%s %s",camelName, goType)
			}
			waitForSemilicon = true
		}
	}	
	fmt.Fprintf(entityWriter, ") (*%s, error) {\n",entityName)

	var bufferInsertSql bytes.Buffer
	var bufferInsertValues bytes.Buffer
	var bufferInsertReturning bytes.Buffer
	var bufferInsertParameters bytes.Buffer

	waitForSemiliconWithId := false
	waitForSemiliconWithoutId := false ;
	indexValue := 1
	for _, column := range table.columns {
		camelName := snakeToCamel(column.Name)
		if column.IsPrimary == false {
			if waitForSemiliconWithoutId == false {
				waitForSemiliconWithoutId = true ;
				bufferInsertSql.WriteString(fmt.Sprintf("%s",column.Name))
				bufferInsertValues.WriteString(fmt.Sprintf("$%d",indexValue))
				bufferInsertParameters.WriteString(fmt.Sprintf("%s",camelName))
			} else {
				bufferInsertSql.WriteString(fmt.Sprintf(",%s",column.Name))
				bufferInsertValues.WriteString(fmt.Sprintf(",$%d",indexValue))
				bufferInsertParameters.WriteString(fmt.Sprintf(",%s",camelName))
			}
			indexValue++
		}
		if waitForSemiliconWithId == false {
			waitForSemiliconWithId = true ;
			bufferInsertReturning.WriteString(fmt.Sprintf("%s",column.Name))
		} else {
			bufferInsertReturning.WriteString(fmt.Sprintf(",%s",column.Name))
		}
	}	
	fmt.Fprintf(entityWriter, "\trows := db.QueryRow(\"insert into %s(%s) values(%s) returning %s\",%s)\n\n",table.Name, bufferInsertSql.String(), bufferInsertValues.String(), bufferInsertReturning.String(), bufferInsertParameters.String())
	fmt.Fprintf(entityWriter, "\t%s, err := rowResultSetTo%s(rows)\n",camelFirstLowEntityName,entityName)
	fmt.Fprintf(entityWriter, "\tif err != nil {\n")
	fmt.Fprintf(entityWriter, "\t\treturn nil, err\n")
	fmt.Fprintf(entityWriter, "\t}\n")
	fmt.Fprintf(entityWriter, "\treturn %s, nil\n",camelFirstLowEntityName)
	fmt.Fprintf(entityWriter, "}\n")
	
	entityWriter.Flush()

	return nil
}

func readFile(configFilename string) (string, error) {
	contentOfFile, err := ioutil.ReadFile(configFilename)
	if err != nil {
		return "", err
	}
	return string(contentOfFile), nil
}

func snakeToCamel(s string) string {
	words := strings.Split(s, "_")
	for i := range words {
		words[i] = strings.Title(words[i])
	}
	return strings.Join(words, "")
}
