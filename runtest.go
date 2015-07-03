package main

import (
    "fmt"
    "sync"
    "math/rand"
    "database/sql"
    _ "github.com/lib/pq"
)

// const (
//     DB_USER     = "vagrant"
//     DB_NAME     = "postgres"
//     DB_PORT     = "4444"
//     TRANSFER_CONNECTIONS = 1
//     BALANCE_CONNECTIONS  = 1
// )

const (
    DB_USER     = "stas1"
    DB_NAME     = "postgres"
    DB_PORT     = "15432"
    TRANSFER_CONNECTIONS = 10
    BALANCE_CONNECTIONS  = 1
)

// const (
//     DB_USER     = "stas1"
//     DB_NAME     = "postgres"
//     DB_PORT     = "5432"
//     TRANSFER_CONNECTIONS = 10
//     BALANCE_CONNECTIONS  = 10
// )

var dbinfo = fmt.Sprintf("user=%s dbname=%s port=%s sslmode=disable", DB_USER, DB_NAME, DB_PORT)

func get_balance(wg *sync.WaitGroup, th_id int) {
    db, err := sql.Open("postgres", dbinfo)
    checkErr(err)
    defer db.Close()

    balance := 0
    new_balance := 0
    for {
        err := db.QueryRow("SELECT sum(balance) FROM accounts").Scan(&new_balance)
        checkErr(err)
        if new_balance != balance && th_id == 0 {
            fmt.Println(balance, "->", new_balance)
            balance = new_balance
        }
    }

    wg.Done()
}

func transfer_money(wg *sync.WaitGroup, th_id int) {
    db, err := sql.Open("postgres", dbinfo)
    checkErr(err)
    defer db.Close()

    for {
        tx, err := db.Begin()
        checkErr(err)
        defer tx.Rollback()

        id1 := rand.Intn(9999)+1;
        id2 := rand.Intn(9999)+1;
        amount := rand.Intn(100000);

        stmt, err := tx.Prepare("UPDATE accounts SET balance = balance + $1 WHERE id=$2")
        checkErr(err)

        _, err = stmt.Exec(amount, id1)
        checkErr(err)

        _, err = stmt.Exec(-1*amount, id2)
        checkErr(err)

        err = tx.Commit()
        checkErr(err)
    }

    wg.Done()
}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}

func main() {
    var wg sync.WaitGroup

    wg.Add(TRANSFER_CONNECTIONS)
    for i:=0; i<TRANSFER_CONNECTIONS; i++{
        go transfer_money(&wg, i)
    }

    wg.Add(BALANCE_CONNECTIONS)
    for i:=0; i<BALANCE_CONNECTIONS; i++{
        go get_balance(&wg, i)
    }

    wg.Wait()
}

