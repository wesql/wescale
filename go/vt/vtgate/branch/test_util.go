package branch

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func NewMockMysqlService(t *testing.T) (*NativeMysqlService, sqlmock.Sqlmock) {
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
			"Users":      "create table if not exists `Users` (\n\tUserID INT primary key,\n\tUsername VARCHAR(50) not null,\n\tEmail VARCHAR(100) not null unique,\n\tPasswordHash VARCHAR(255) not null\n)",
			"Orders":     "create table if not exists Orders (\n\tOrderID INT primary key,\n\tUserID INT,\n\tOrderDate DATETIME,\n\t`Status` VARCHAR(20),\n\tShippingAddress VARCHAR(255),\n\tforeign key (UserID) references `Users` (UserID)\n)",
			"OrderItems": "create table if not exists OrderItems (\n\tOrderItemID INT primary key,\n\tOrderID INT,\n\tProductID INT,\n\tQuantity INT,\n\tPrice DECIMAL(10,2),\n\tforeign key (OrderID) references Orders (OrderID)\n)",
		},
		"Inventory": {
			"Products":     "create table if not exists Products (\n\tProductID INT primary key,\n\tProductName VARCHAR(100) not null,\n\tCategoryID INT,\n\tPrice DECIMAL(10,2),\n\tStock INT\n)",
			"Categories":   "create table if not exists Categories (\n\tCategoryID INT primary key,\n\tCategoryName VARCHAR(50) not null\n)",
			"Suppliers":    "create table if not exists Suppliers (\n\tSupplierID INT primary key,\n\tSupplierName VARCHAR(100),\n\tContactEmail VARCHAR(100)\n)",
			"InventoryLog": "create table if not exists InventoryLog (\n\tLogID INT primary key,\n\tProductID INT,\n\tChangeAmount INT,\n\tChangeDate DATETIME,\n\tforeign key (ProductID) references Products (ProductID)\n)",
		},
		"HR": {
			"Employees":   "create table if not exists Employees (\n\tEmployeeID INT primary key,\n\tFirstName VARCHAR(50),\n\tLastName VARCHAR(50),\n\tDepartmentID INT,\n\tEmail VARCHAR(100) unique\n)",
			"Departments": "create table if not exists Departments (\n\tDepartmentID INT primary key,\n\tDepartmentName VARCHAR(100)\n)",
			"Payroll":     "create table if not exists Payroll (\n\tPayrollID INT primary key,\n\tEmployeeID INT,\n\tSalary DECIMAL(15,2),\n\tPayrollDate DATE,\n\tforeign key (EmployeeID) references Employees (EmployeeID)\n)",
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

var (
	BranchMetaColumns = []string{
		"Name",
		"source_host",
		"source_port",
		"source_user",
		"source_password",
		"include_databases",
		"exclude_databases",
		"target_db_pattern",
		"Status",
	}

	BranchMetasForTest = []*BranchMeta{
		{
			Name:             "test0",
			SourceHost:       "prod.mysql.example.com",
			SourcePort:       3306,
			SourceUser:       "repl_user",
			SourcePassword:   "password123",
			IncludeDatabases: []string{"db1", "db2"},
			ExcludeDatabases: []string{"db3"},
			Status:           "init",
		},
		{
			Name:             "test1",
			SourceHost:       "prod.mysql.example.com",
			SourcePort:       3306,
			SourceUser:       "repl_user",
			SourcePassword:   "password123",
			IncludeDatabases: []string{"*"},
			ExcludeDatabases: []string{},
			Status:           "bad Status",
		},
		{
			Name:             "test2",
			SourceHost:       "prod.mysql.example.com",
			SourcePort:       3306,
			SourceUser:       "repl_user",
			SourcePassword:   "password123",
			IncludeDatabases: []string{"*"},
			ExcludeDatabases: []string{},
			Status:           "unknown",
		},
	}
)

func InitMockBranchMetas(mock sqlmock.Sqlmock) {

	for i, _ := range BranchMetasForTest {
		rows := sqlmock.NewRows(BranchMetaColumns).AddRow(
			BranchMetasForTest[i].Name,
			BranchMetasForTest[i].SourceHost,
			BranchMetasForTest[i].SourcePort,
			BranchMetasForTest[i].SourceUser,
			BranchMetasForTest[i].SourcePassword,
			strings.Join(BranchMetasForTest[i].IncludeDatabases, ","),
			strings.Join(BranchMetasForTest[i].ExcludeDatabases, ","),
			BranchMetasForTest[i].Status,
		)

		mock.ExpectQuery(fmt.Sprintf("select * from mysql.branch where Name='test%d'", i)).WillReturnRows(rows)
	}

	for _, meta := range BranchMetasForTest {
		meta.Status = StringToBranchStatus(string(meta.Status))
	}
}
