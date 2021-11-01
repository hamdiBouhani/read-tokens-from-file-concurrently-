
to test this code you need to create database containe this table `tokens` and add .env file in the root of folder:

## sql-schema

```sql
CREATE TABLE tokens
(
    token    character(7) NOT NULL,
    token_count       BIGINT DEFAULT 1,

    created_date     timestamp DEFAULT now(),
    changed_date     timestamp,
    deleted_date     timestamp,
    CONSTRAINT tokens_pk PRIMARY KEY (token)
);
```

## .env file:

```
DB_MIGRATE=false
DB_USER=postgres
DB_PASS=postgres
DB_URL=localhost
DB_DATABASE=phdp
DB_LOGS=true
DB_PORT=5432
DB_SSL=
```


# Resources:
## list of artical used as resource: 
[How to generate a random string of a fixed length in Go?](https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go)

[Reading 16GB File in Seconds, Golang](https://medium.com/swlh/processing-16gb-file-in-seconds-go-lang-3982c235dfa2)

[Write string slice line by line to a text file](https://www.golangprograms.com/write-string-slice-line-by-line-to-a-text-file.html)


the `main.go` file:

containe the solution for the challenge

### RandStringBytes() function:
* generate a rondom token.
### WriteTokensIntoDB(fileName string) function:
* upsert tokens into database.
### WriteTokensIntoTextFile(fileName string) function:
* Write token line by line to a text file.

