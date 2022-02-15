package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	DbUrl string
	Db    sqlx.DB
)

func main() {
	Db = *initDb()
	defer Db.Close()
	api_data := extractDataFromApi()
	insertDataIntoDb(api_data)
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

func extractDataFromApi() map[string]map[string]map[string]interface{} {
	ApiUrl := fmt.Sprintf(
		"https://freecurrencyapi.net/api/v2/historical?apikey=%s&date_from=%s&date_to=%s",
		os.Getenv("API_TOKEN"),
		"2020-01-02",
		"2020-12-30",
	)
	res, err := http.Get(ApiUrl)
	if err != nil {
		panic(err.Error())
	}
	defer res.Body.Close()
	byteValue, _ := ioutil.ReadAll(res.Body)
	var api_data map[string]map[string]map[string]interface{}
	_ = json.Unmarshal(byteValue, &api_data)
	return api_data
}

func insertDataIntoDb(api_data map[string]map[string]map[string]interface{}) {
	for key, element := range api_data {
		if key == "data" {
			for key2, element2 := range element {
				counter := 0
				valueStrings := []string{}
				valueArgs := []interface{}{}
				for key3, element3 := range element2 {
					str1 := "$" + strconv.Itoa(1+counter*4) + ","
					str2 := "$" + strconv.Itoa(2+counter*4) + ","
					str3 := "$" + strconv.Itoa(3+counter*4) + ","
					str4 := "$" + strconv.Itoa(4+counter*4)
					str_n := "(" + str1 + str2 + str3 + str4 + ")"
					valueStrings = append(valueStrings, str_n)
					valueArgs = append(valueArgs, key2)
					valueArgs = append(valueArgs, "EUR")
					valueArgs = append(valueArgs, key3)
					valueArgs = append(valueArgs, element3)
					counter = counter + 1
				}
				smt := `
					INSERT INTO currencies (
						date,
						base_currency,
						currency,
						price
					)
					VALUES %s
					ON CONFLICT (date, currency) DO NOTHING;`
				smt = fmt.Sprintf(smt, strings.Join(valueStrings, ","))
				_, err := Db.Exec(smt, valueArgs...)
				if err != nil {
					panic(err.Error())
				}
			}
		}
	}
}
