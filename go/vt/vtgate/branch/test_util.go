package branch

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func NewMockMysqlService(t *testing.T) (*MysqlService, sqlmock.Sqlmock) {
	// use QueryMatcherEqual to match exact query
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))

	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}

	service, err := NewMysqlService(db)
	if err != nil {
		t.Fatalf("failed to create mysql service: %v", err)
	}

	// if not set, when the current expected query does not match your actual query, it will fail
	// instead of trying to match all.
	mock.MatchExpectationsInOrder(false)

	return service, mock
}

var BranchSchemaForTest = &BranchSchema{
	branchSchema: map[string]map[string]string{
		"eCommerce": {
			"Users": `
                    CREATE TABLE Users (
                        UserID INT PRIMARY KEY,
                        Username VARCHAR(50) NOT NULL,
                        Email VARCHAR(100) NOT NULL UNIQUE,
                        PasswordHash VARCHAR(255) NOT NULL
                    );`,
			"Orders": `
                    CREATE TABLE Orders (
                        OrderID INT PRIMARY KEY,
                        UserID INT,
                        OrderDate DATETIME,
                        Status VARCHAR(20),
                        ShippingAddress VARCHAR(255),
                        FOREIGN KEY (UserID) REFERENCES Users(UserID)
                    );`,
			"OrderItems": `
                    CREATE TABLE OrderItems (
                        OrderItemID INT PRIMARY KEY,
                        OrderID INT,
                        ProductID INT,
                        Quantity INT,
                        Price DECIMAL(10, 2),
                        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
                    );`,
		},
		"Inventory": {
			"Products": `
                    CREATE TABLE Products (
                        ProductID INT PRIMARY KEY,
                        ProductName VARCHAR(100) NOT NULL,
                        CategoryID INT,
                        Price DECIMAL(10, 2),
                        Stock INT
                    );`,
			"Categories": `
                    CREATE TABLE Categories (
                        CategoryID INT PRIMARY KEY,
                        CategoryName VARCHAR(50) NOT NULL
                    );`,
			"Suppliers": `
                    CREATE TABLE Suppliers (
                        SupplierID INT PRIMARY KEY,
                        SupplierName VARCHAR(100),
                        ContactEmail VARCHAR(100)
                    );`,
			"InventoryLog": `
                    CREATE TABLE InventoryLog (
                        LogID INT PRIMARY KEY,
                        ProductID INT,
                        ChangeAmount INT,
                        ChangeDate DATETIME,
                        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
                    );`,
		},
		"HR": {
			"Employees": `
                    CREATE TABLE Employees (
                        EmployeeID INT PRIMARY KEY,
                        FirstName VARCHAR(50),
                        LastName VARCHAR(50),
                        DepartmentID INT,
                        Email VARCHAR(100) UNIQUE
                    );`,
			"Departments": `
                    CREATE TABLE Departments (
                        DepartmentID INT PRIMARY KEY,
                        DepartmentName VARCHAR(100)
                    );`,
			"Payroll": `
                    CREATE TABLE Payroll (
                        PayrollID INT PRIMARY KEY,
                        EmployeeID INT,
                        Salary DECIMAL(15, 2),
                        PayrollDate DATE,
                        FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
                    );`,
		},
	},
}

func InitMockShowDatabases(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{"Database"})
	for db, _ := range BranchSchemaForTest.branchSchema {
		rows = rows.AddRow(db)
	}
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
}

func InitMockShowCreateTable(mock sqlmock.Sqlmock) {
	for db, tables := range BranchSchemaForTest.branchSchema {
		for table, createTableStmt := range tables {
			sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", db, table)
			println(sql)
			row := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(table, createTableStmt)
			mock.ExpectQuery(sql).WillReturnRows(row)
		}
	}
}

func InitMockTableInfos(mock sqlmock.Sqlmock) {
	query1 := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
	rows1 := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for db, tables := range BranchSchemaForTest.branchSchema {
		for table, _ := range tables {
			rows1 = rows1.AddRow(db, table)
		}
	}
	mock.ExpectQuery(query1).WillReturnRows(rows1)

	query2 := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('eCommerce')"
	rows2 := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for db, tables := range BranchSchemaForTest.branchSchema {
		if db == "eCommerce" {
			continue
		}
		for table, _ := range tables {
			rows2 = rows2.AddRow(db, table)
		}
	}
	mock.ExpectQuery(query2).WillReturnRows(rows2)

	query3 := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('eCommerce')"
	rows3 := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for db, tables := range BranchSchemaForTest.branchSchema {
		if db == "eCommerce" {
			for table, _ := range tables {
				rows3 = rows3.AddRow(db, table)
			}
		}
	}
	mock.ExpectQuery(query3).WillReturnRows(rows3)
}

var (
	BranchMetaColumns = []string{
		"name",
		"source_host",
		"source_port",
		"source_user",
		"source_password",
		"include_databases",
		"exclude_databases",
		"target_db_pattern",
		"status",
		"id_of_next_ddl_to_execute",
	}

	BranchMetasForTest = []*BranchMeta{
		{
			name:                 "test0",
			sourceHost:           "prod.mysql.example.com",
			sourcePort:           3306,
			sourceUser:           "repl_user",
			sourcePassword:       "password123",
			includeDatabases:     []string{"db1", "db2"},
			excludeDatabases:     []string{"db3"},
			targetDBPattern:      "",
			status:               "init",
			idOfNextDDLToExecute: 0,
		},
		{
			name:                 "test1",
			sourceHost:           "prod.mysql.example.com",
			sourcePort:           3306,
			sourceUser:           "repl_user",
			sourcePassword:       "password123",
			includeDatabases:     []string{"*"},
			excludeDatabases:     []string{},
			targetDBPattern:      "",
			status:               "bad status",
			idOfNextDDLToExecute: 0,
		},
		{
			name:                 "test2",
			sourceHost:           "prod.mysql.example.com",
			sourcePort:           3306,
			sourceUser:           "repl_user",
			sourcePassword:       "password123",
			includeDatabases:     []string{},
			excludeDatabases:     []string{},
			targetDBPattern:      "",
			status:               "unknown",
			idOfNextDDLToExecute: 0,
		},
	}
)

func InitMockBranchMetas(mock sqlmock.Sqlmock) {

	for i, _ := range BranchMetasForTest {
		rows := sqlmock.NewRows(BranchMetaColumns).AddRow(
			BranchMetasForTest[i].name,
			BranchMetasForTest[i].sourceHost,
			BranchMetasForTest[i].sourcePort,
			BranchMetasForTest[i].sourceUser,
			BranchMetasForTest[i].sourcePassword,
			strings.Join(BranchMetasForTest[i].includeDatabases, ","),
			strings.Join(BranchMetasForTest[i].excludeDatabases, ","),
			BranchMetasForTest[i].targetDBPattern,
			BranchMetasForTest[i].status,
			BranchMetasForTest[i].idOfNextDDLToExecute,
		)

		mock.ExpectQuery(fmt.Sprintf("select * from mysql.branch where name='test%d'", i)).WillReturnRows(rows)
	}

	for _, meta := range BranchMetasForTest {
		meta.status = StringToBranchStatus(string(meta.status))
	}
}
