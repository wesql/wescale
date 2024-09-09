/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const (
	tableSchema = "d1"
	tableName   = "accounts1"

	initAccountsNumber = 10000
	initMoney          = 10000

	threadNumber       = 5
	loopNumber         = 5000
	timeGapBetweenLoop = 1 * time.Millisecond
	// each is in [0,1], sum of them should be 1
	InsertProbability = 0.1
	deleteProbability = 0.1
	updateProbability = 0.8
)

func TestInitAccounts(t *testing.T) {
	dsn := fmt.Sprintf("(%s:%d)/%s", host, port, table1Schema)
	db, err := sql.Open("mysql", dsn)
	assert.Nil(t, err)
	defer db.Close()

	dropQuery := fmt.Sprintf("drop table if exists %s.%s", tableSchema, tableName)
	_, err = db.Exec(dropQuery)

	createQuery := fmt.Sprintf("create table if not exists %s.%s (id int primary key, money int)", tableSchema, tableName)
	_, err = db.Exec(createQuery)
	assert.Nil(t, err)

	for i := 0; i < initAccountsNumber; i++ {
		err = createAccount(db, i, initMoney)
		assert.Nil(t, err)
	}
}

func TestTransfer(t *testing.T) {
	wg := &sync.WaitGroup{}
	testTransfer := func() {
		defer wg.Done()
		dsn := fmt.Sprintf("(%s:%d)/%s", host, port, table1Schema)
		db, err := sql.Open("mysql", dsn)
		assert.Nil(t, err)
		defer db.Close()
		for i := 0; i < loopNumber; i++ {
			switch randomWithProbability() {
			case 0:
				// insert new account which id greater than initAccountsNumber
				err := createAccount(db, rand.Intn(loopNumber)+initAccountsNumber, initMoney)
				assert.Nil(t, err)
			case 1:
				// delete account which id less than initAccountsNumber
				err := deleteAccount(db, rand.Intn(initAccountsNumber))
				assert.Nil(t, err)
			case 2:
				// transfer money between accounts which id less than initAccountsNumber
				err := transferMoney(db, rand.Intn(initAccountsNumber), rand.Intn(initAccountsNumber), 100)
				assert.Nil(t, err)
			}
			time.Sleep(timeGapBetweenLoop)
		}
	}
	for i := 0; i < threadNumber; i++ {
		wg.Add(1)
		go testTransfer()
	}
	wg.Wait()
}

func createAccount(db *sql.DB, id, money int) error {
	query := fmt.Sprintf("insert ignore into %s.%s (id,money) values(%d,%d)", tableSchema, tableName, id, money)
	_, err := db.Exec(query)
	return err
}

func deleteAccount(db *sql.DB, id int) error {
	query := fmt.Sprintf("delete from %s.%s where id = %d", tableSchema, tableName, id)
	_, err := db.Exec(query)
	return err
}

func transferMoney(db *sql.DB, sender, reciever, money int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	subMoney := fmt.Sprintf("update %s.%s set money = money - %d where id = %d", tableSchema, tableName, money, sender)
	_, err = tx.Exec(subMoney)
	if err != nil {
		return err
	}
	addMoney := fmt.Sprintf("update %s.%s set money = money + %d where id = %d", tableSchema, tableName, money, reciever)
	_, err = tx.Exec(addMoney)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func randomWithProbability() int {
	if InsertProbability+deleteProbability+updateProbability != 1.0 {
		panic("Probabilities must sum to 1")
	}

	r := rand.Float64()
	if r <= InsertProbability {
		return 0
	} else if r <= InsertProbability+deleteProbability {
		return 1
	} else {
		return 2
	}
}
