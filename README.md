This script models simple bank account transfers. 10 workers are moving money from user to user, 1 connection is checking total amount of money. If overall amount is changed than script will print old and new values.

```shell
> psql -p 15432 postgres < init.sql 
DROP TABLE
CREATE TABLE
INSERT 0 10000
    sum    
-----------
 501281886
(1 row)

> go run runtest.go                
0 -> 501281886
501281886 -> 501379459
501379459 -> 501281886
501281886 -> 501208451
501208451 -> 501208316
501208316 -> 501208451
```
