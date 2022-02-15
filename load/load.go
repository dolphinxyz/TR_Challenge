package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/thoas/go-funk"
)

var (
	DbUrl string
	Db    sqlx.DB
)

type Instrument struct {
	Id             int    `csv:"instrument_id"`
	SectorName     string `csv:"sector_name"`
	CountryName    string `csv:"country_name"`
	IndexName      string `csv:"index_name"`
	InstrumentType string `csv:"instrument_type"`
}

type Price struct {
	Date         string  `csv:"date"`
	Price        float64 `csv:"price"`
	InstrumentId string  `csv:"instrument_id"`
}

type Trade struct {
	CustomerId     int     `csv:"customer_id"`
	ExecutionTime  string  `csv:"execution_time"`
	Direction      string  `csv:"direction"`
	ExecutionSize  float64 `csv:"execution_size"`
	ExecutionPrice float64 `csv:"execution_price"`
	InstrumentId   int     `csv:"instrument_id"`
}

func main() {
	Db = *initDb()
	defer Db.Close()

	runInitSql()
	loadInstrumentsData()
	loadPricesData()
	loadTradesData()
}

func initDb() (db *sqlx.DB) {
	DbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		os.Getenv("TL_DB_USER"),
		os.Getenv("TL_DB_PASS"),
		os.Getenv("TL_DB_HOST"),
		os.Getenv("TL_DB_PORT"),
		os.Getenv("TL_DB_NAME"),
	)
	db, err := sqlx.Connect("postgres", DbUrl)
	if err != nil {
		panic(err.Error())
	}
	return db
}

func runInitSql() {
	path := filepath.Join("./init.sql")
	c, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err.Error())
	}
	initSql := string(c)
	_, err = Db.Exec(initSql)
	if err != nil {
		panic(err.Error())
	}
}

func loadInstrumentsData() {
	fmt.Println("Starting loading instruments...")
	filename := "instruments.csv"
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	instruments := []*Instrument{}

	if err := gocsv.UnmarshalFile(f, &instruments); err != nil {
		panic(err)
	}

	valueStrings := []string{}
	valueArgs := []interface{}{}
	for i, elem := range instruments {
		str1 := "$" + strconv.Itoa(1+i*5) + ","
		str2 := "$" + strconv.Itoa(2+i*5) + ","
		str3 := "$" + strconv.Itoa(3+i*5) + ","
		str4 := "$" + strconv.Itoa(4+i*5) + ","
		str5 := "$" + strconv.Itoa(5+i*5)
		str_n := "(" + str1 + str2 + str3 + str4 + str5 + ")"
		valueStrings = append(valueStrings, str_n)
		valueArgs = append(valueArgs, elem.Id)
		valueArgs = append(valueArgs, elem.SectorName)
		valueArgs = append(valueArgs, elem.CountryName)
		valueArgs = append(valueArgs, elem.IndexName)
		valueArgs = append(valueArgs, elem.InstrumentType)
	}
	smt := `
		INSERT INTO instruments (
			instrument_id,
			sector_name,
			country_name,
			index_name,
			instrument_type
		)
		VALUES %s
		ON CONFLICT (instrument_id) DO NOTHING;`
	smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))

	_, err = Db.Exec(smt, valueArgs...)
	if err != nil {
		panic(err.Error())
	}
}

func loadPricesData() {
	fmt.Println("Starting loading prices...")
	filename := "prices.csv"
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	prices := []*Price{}
	if err := gocsv.UnmarshalFile(f, &prices); err != nil {
		panic(err)
	}

	chunkList := funk.Chunk(prices, 10000)
	for _, chunk := range chunkList.([][]*Price) {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for i, elem := range chunk {
			str1 := "$" + strconv.Itoa(1+i*3) + ","
			str2 := "$" + strconv.Itoa(2+i*3) + ","
			str3 := "$" + strconv.Itoa(3+i*3)
			str_n := "(" + str1 + str2 + str3 + ")"
			valueStrings = append(valueStrings, str_n)
			valueArgs = append(valueArgs, elem.Date)
			valueArgs = append(valueArgs, elem.Price)
			valueArgs = append(valueArgs, elem.InstrumentId)
		}
		smt := `
		INSERT INTO prices (
			date,
			price,
			instrument_id
		)
		VALUES %s
		ON CONFLICT (date, instrument_id) DO NOTHING;`
		smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))

		_, err = Db.Exec(smt, valueArgs...)
		if err != nil {
			panic(err.Error())
		}
	}
}

func loadTradesData() {
	fmt.Println("Starting loading trades...")
	filename := "trades.csv"
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	trades := []*Trade{}
	if err := gocsv.UnmarshalFile(f, &trades); err != nil {
		panic(err)
	}

	chunkList := funk.Chunk(trades, 10000)
	for _, chunk := range chunkList.([][]*Trade) {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for i, elem := range chunk {
			str1 := "$" + strconv.Itoa(1+i*6) + ","
			str2 := "$" + strconv.Itoa(2+i*6) + ","
			str3 := "$" + strconv.Itoa(3+i*6) + ","
			str4 := "$" + strconv.Itoa(4+i*6) + ","
			str5 := "$" + strconv.Itoa(5+i*6) + ","
			str6 := "$" + strconv.Itoa(6+i*6)
			str_n := "(" + str1 + str2 + str3 + str4 + str5 + str6 + ")"
			valueStrings = append(valueStrings, str_n)
			valueArgs = append(valueArgs, elem.CustomerId)
			valueArgs = append(valueArgs, elem.ExecutionTime)
			valueArgs = append(valueArgs, elem.Direction)
			valueArgs = append(valueArgs, elem.ExecutionSize)
			valueArgs = append(valueArgs, elem.ExecutionPrice)
			valueArgs = append(valueArgs, elem.InstrumentId)
		}
		smt := `
		INSERT INTO trades (
			customer_id,
			execution_time,
			direction,
			execution_size,
			execution_price,
			instrument_id
		)
		VALUES %s
		ON CONFLICT (customer_id, execution_time) DO NOTHING;`
		smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))

		_, err = Db.Exec(smt, valueArgs...)
		if err != nil {
			panic(err.Error())
		}
	}
}
