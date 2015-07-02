package main

import (
    "database/sql"
    "fmt"
    _ "github.com/lib/pq"
    // "time"
    "sync"
    "math/rand"
)

const (
    DB_USER     = "stas"
    DB_NAME     = "postgres"
    DB_PORT     = "15432"
)

func populate(dbinfo string) {
    db, err := sql.Open("postgres", dbinfo)
    checkErr(err)
    
    fmt.Println("Connected.")

    tx, err := db.Begin()
    checkErr(err)
    defer tx.Rollback()
    
    stmt, err := tx.Prepare("INSERT INTO accounts(user_id, balance) VALUES($1, $2)")
    checkErr(err)

    for i:=0; i < 10000; i++ {
        _, err := stmt.Exec(42*i, rand.Intn(100000))
        checkErr(err)
    }

    err = tx.Commit()
    checkErr(err)

    db.Close()
}

func get_balance(dbinfo string) int {
    db, err := sql.Open("postgres", dbinfo)
    checkErr(err)

    var balance int
    err = db.QueryRow("SELECT sum(balance) FROM accounts").Scan(&balance)
    checkErr(err)

    db.Close()

    return balance
}

func transfer_money(wg *sync.WaitGroup, th_id int, dbinfo string) {
    db, err := sql.Open("postgres", dbinfo)
    checkErr(err)
    var i = 0    

    for i<20000 {
        tx, err := db.Begin()
        checkErr(err)
        defer tx.Rollback()

        var amount int
        var id1, id2 int

        id1 = rand.Intn(9990)+1;
        id2 = rand.Intn(9990)+1;
        amount = rand.Intn(99990)+1;

        stmt, err := tx.Prepare("UPDATE accounts SET balance = balance + $1 WHERE id=$2")
        checkErr(err)

        _, err = stmt.Exec(amount, id1)
        checkErr(err)

        _, err = stmt.Exec(-1*amount, id2)
        checkErr(err)

        err = tx.Commit()
        checkErr(err)

        i += 1

        if i%1000 == 0 {
            fmt.Sprintf("Goroutine %s. %s transactions commited. Total = %s", th_id, i, get_balance(dbinfo))
        }
    }

    db.Close()

    wg.Done()

}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}

func main() {
    dbinfo := fmt.Sprintf("user=%s dbname=%s port=%s sslmode=disable", DB_USER, DB_NAME, DB_PORT)
    fmt.Println(get_balance(dbinfo))

    connections := 15

    var wg sync.WaitGroup
    wg.Add(connections) 

    for i:=0; i<connections; i++{
        go transfer_money(&wg, i, dbinfo)
    }

    wg.Wait()
    fmt.Println("Finished")
}



