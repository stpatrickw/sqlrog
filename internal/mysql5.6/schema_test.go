package mysql

import (
	"fmt"
	_ "github.com/nakagami/firebirdsql"
	sqlrog "github.com/stpatrickw/sqlrog/internal/sqlrog"
	"os"
	"strings"
	"testing"
)

var (
	myEngine     MysqlEngine
	sourceSchema sqlrog.ElementSchema
	targetSchema sqlrog.ElementSchema
	sourceConfig sqlrog.Config
)

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	os.Exit(code)
}

func setUp() {
	sourceConfig = sqlrog.Config{
		AppName: "test_db",
		Engine:  "mysql5.6",
		AppType: "project",
		Params: sqlrog.ConfigParams{
			FileType: "yml",
		},
	}
	reloadSchemas()
}

func reloadSchemas() {
	schema, err := myEngine.LoadSchema(&sourceConfig, &sqlrog.YamlSchemaReader{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	sourceSchema = schema
	schema, err = myEngine.LoadSchema(&sourceConfig, &sqlrog.YamlSchemaReader{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	targetSchema = schema
}

func TestTableCreateSQL(t *testing.T) {
	targetSchema = &MysqlSchema{}

	expectedSqls := []string{
		`CREATE TABLE categories (
	id int(11) NOT NULL auto_increment,
	serial int(11) NOT NULL,
	color varchar(45) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	colorname varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
	id_engine int(11),
	id_producer int(11),
	PRIMARY KEY(id,serial)
) Engine=InnoDB CHARSET=latin1;`,
		`CREATE TABLE cars (
	id int(11) NOT NULL auto_increment,
	name varchar(45) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'no name',
	id_category int(11) NOT NULL,
	speed int(11) COMMENT 'describes speed',
	weight int(11),
	serial int(11) NOT NULL,
	producer varchar(45) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	PRIMARY KEY(id)
) Engine=InnoDB CHARSET=latin1;`,
		`CREATE TABLE engines (
	id int(11) NOT NULL auto_increment,
	name varchar(45) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	PRIMARY KEY(id)
) Engine=InnoDB CHARSET=latin1;`,
		`CREATE TABLE producers (
	id int(11) NOT NULL auto_increment,
	name varchar(45) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	PRIMARY KEY(id)
) Engine=InnoDB CHARSET=latin1;`,
		`CREATE INDEX fk_cars1_idx ON cars (id_category,serial);`,
		`CREATE INDEX idx_2 ON cars (speed,weight);`,
		`ALTER TABLE cars ADD CONSTRAINT fk_cars1 FOREIGN KEY (id_category,serial) REFERENCES categories (id,serial) ON DELETE NO ACTION ON UPDATE NO ACTION;`,
		`CREATE TRIGGER cars_BEFORE_INSERT BEFORE INSERT ON cars FOR EACH ROW
BEGIN
	SET NEW.weight = 18;
END;`,
		`CREATE INDEX idx_1 ON cars (name);`,
		`CREATE INDEX fk_categories2_idx ON categories (id_producer);`,
		`CREATE INDEX fk_categories1_idx ON categories (id_engine);`,
		`ALTER TABLE categories ADD CONSTRAINT fk_categories1 FOREIGN KEY (id_engine) REFERENCES engines (id) ON DELETE NO ACTION ON UPDATE NO ACTION;`,
		`ALTER TABLE categories ADD CONSTRAINT fk_categories2 FOREIGN KEY (id_producer) REFERENCES producers (id) ON DELETE CASCADE ON UPDATE CASCADE;`,
		`ALTER TABLE cars ADD CONSTRAINT serial_UNIQUE UNIQUE (serial);`,
		`CREATE OR REPLACE VIEW cars_view 
as select * from cars;`,
		`CREATE FUNCTION 1plus(
	arg2 int(11)) RETURNS int(11)
BEGIN

RETURN 1 + @arg2;
END;`,
		`CREATE PROCEDURE GetAllCarsByColor(
	IN ColorName varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci)
BEGIN
    select * from cars where color = @ColorName;
 END;`,
	}
	changes := myEngine.SchemaDiff(sourceSchema, targetSchema)

	for _, diff := range changes {
		for _, diffSql := range diff.DiffSql(";") {
			for index, sql := range expectedSqls {
				if diffSql == sql {
					expectedSqls = append(expectedSqls[:index], expectedSqls[index+1:]...)
				}
			}
		}
	}
	if len(expectedSqls) > 0 {
		t.Errorf("Expected sql is missing in diff: \n%s\n\n", strings.Join(expectedSqls, "\n"))
	}
}

func TestTableDropDiff(t *testing.T) {
	reloadSchemas()
	tableName := "engines"
	for typeElem, elements := range sourceSchema.(*MysqlSchema).CoreElements {
		if typeElem == "table" {
			delete(elements, tableName)
		}
	}
	changes := myEngine.SchemaDiff(sourceSchema, targetSchema)
	if len(changes) == 0 || changes[0].State != sqlrog.DIFF_TYPE_DROP {
		t.Errorf("Expected drop table diff is missing for table: %s\n", tableName)
	}
}

func TestTableAlterSQL(t *testing.T) {
	reloadSchemas()
	tableName := "engines"
	for typeElem, elements := range sourceSchema.(*MysqlSchema).CoreElements {
		if typeElem == "table" {
			table := elements[tableName].(*Table)
			table.Fields["volume"] = &TableColumn{
				BaseElementSchema: sqlrog.BaseElementSchema{},
				Name:              "volume",
				Type:              "int",
			}
		}
	}
	changes := myEngine.SchemaDiff(sourceSchema, targetSchema)
	if len(changes) == 0 || changes[0].State != sqlrog.DIFF_TYPE_UPDATE {
		t.Errorf("Expected update table diff is missing for table: %s\n", tableName)
	}
	sql := changes[0].DiffSql(sqlrog.DEFAULT_SQL_SEPARATOR)
	expectedSQL := "ALTER TABLE engines ADD COLUMN volume int;"
	if sql[0] != expectedSQL {
		t.Errorf("Expected update table sql is not equal to real: \n%s\n%s\n", expectedSQL, sql[0])
	}
}
