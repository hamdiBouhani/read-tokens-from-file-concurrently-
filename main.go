package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"

	_ "github.com/jinzhu/gorm/dialects/postgres"

	"github.com/joho/godotenv"
)



const letterBytes = "abcdefghijklmnopqrstuvwxy"

type DBService struct {
	Db *gorm.DB
}

func ConfigureDatastore() (*gorm.DB, error) {

	log.Println("Using Postgres Database")
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}

	ssl := os.Getenv("DB_SSL")
	if ssl == "" {
		ssl = "disable"
	}

	db, err := gorm.Open(
		"postgres",
		fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s password=%s",
			os.Getenv("DB_URL"),
			port,
			os.Getenv("DB_USER"),
			os.Getenv("DB_DATABASE"),
			ssl,
			os.Getenv("DB_PASS"),
		),
	)

	if err != nil {
		return nil, err
	}

	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	db.DB().SetConnMaxLifetime(5 * time.Second)

	db.LogMode(false)
	if os.Getenv("DB_LOGS") == "true" {
		db.LogMode(true)
	}

	return db, nil
}

func (svc *DBService) Show(db *gorm.DB, out interface{}, where ...interface{}) *gorm.DB {
	return db.First(out, where...)
}

var dbSVC db.DBService

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandStringBytes() string {
	b := make([]byte, 7)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func Process(f *os.File) error {

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 250*1024)
		return lines
	}}

	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}

	r := bufio.NewReader(f)

	var wg sync.WaitGroup

	for {
		buf := linesPool.Get().([]byte)

		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		// ReadBytes reads until the first occurrence of delim in the input,
		// returning a slice containing the data up to and including the delimiter.
		nextUntillNewline, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func() {
			ProcessChunk(buf, &linesPool, &stringPool)
			wg.Done()
		}()

	}

	wg.Wait()
	return nil
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool) {

	var wg2 sync.WaitGroup

	logs := stringPool.Get().(string)
	logs = string(chunk)

	linesPool.Put(chunk)

	logsSlice := strings.Split(logs, "\n")

	stringPool.Put(logs)

	chunkSize := 300
	n := len(logsSlice)
	noOfThread := n / chunkSize

	if n%chunkSize != 0 {
		noOfThread++
	}

	for i := 0; i < (noOfThread); i++ {

		wg2.Add(1)
		go func(s int, e int) {
			defer wg2.Done() //to avoid deadlocks
			for i := s; i < e; i++ {
				text := logsSlice[i]
				if len(text) == 0 {
					continue
				}
				fmt.Println(text)

				var count = 0
				dbSVC.Db.Table("tokens").Where("token = ?", text).Count(&count)
				if count > 0 {
					var _token Token
					err := dbSVC.Show(dbSVC.Db, &_token, "token = ?", text).Error
					if err != nil {
						fmt.Println("faild to get text", err)
					}

					err = dbSVC.Db.Omit("token").Model(&Token{Token: _token.Token}).Update(&Token{TokenCount: (_token.TokenCount + 1)}).Error
					if err != nil {
						fmt.Println("faild to update token", err)
					}
				} else {
					err := dbSVC.Db.Create(&Token{Token: text, TokenCount: 1}).Error
					if err != nil {
						fmt.Println("faild to insert token", err)
					}
				}
			}

		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(logsSlice)))))
	}

	wg2.Wait()
	logsSlice = nil
}

func WriteTokensIntoDB(fileName string) error {

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("cannot able to read the file", err)
		return err
	}

	defer file.Close() //close after checking err

	err = Process(file)
	if err != nil {
		fmt.Println("cannot process file", err)
		return err
	}

	return nil
}

//Write token line by line to a text file
func WriteTokensIntoTextFile(fileName string) error {

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
		return err
	}

	datawriter := bufio.NewWriter(file)

	for i := 0; i < 10000000; i++ {
		_, _ = datawriter.WriteString(RandStringBytes() + "\n")
	}

	datawriter.Flush()
	file.Close()

	return nil
}

type Token struct {
	Token      string `json:"-" gorm:"primary_key"`
	TokenCount int64  `json:"token_count"`

	CreatedAt time.Time  `json:"-" gorm:"column:created_date"`
	UpdatedAt time.Time  `json:"-" gorm:"column:changed_date"`
	DeletedAt *time.Time `json:"-" gorm:"column:deleted_date"`
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	_db, err := db.ConfigureDatastore()
	if err != nil {
		log.Fatalf("%v", err)
	}
	dbSVC.Db = _db

	t := time.Now()

	fileName := fmt.Sprintf("tokens_%v.txt", t.Format(time.RFC3339))
	err = WriteTokensIntoTextFile(fileName)
	if err != nil {
		log.Fatalf("%v", err)
	}

	err = WriteTokensIntoDB(fileName)
	if err != nil {
		log.Fatalf("%v", err)
	}
}
