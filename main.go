package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	file, err := os.Open("Autoscal Template Structure  - template.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	df := dataframe.ReadCSV(file)

	ids := getID(df)
	fmt.Println(ids)

	df = fillna(df, "ID")

	skills := extractData(df, 1)

	fmt.Println(skills)
}

func fillna(df dataframe.DataFrame, s string) dataframe.DataFrame {
	col_series := df.Col(s)
	col_records := col_series.Records()

	for i := 0; i < len(col_records); i++ {
		if col_series.Elem(i).IsNA() {
			col_records[i] = col_records[i-1]
		}
	}

	new_series := series.New(col_records, series.Int, "ID")

	return df.Mutate(new_series)
}

func getID(df dataframe.DataFrame) []int {
	id := []int{}
	for i := 0; i < len(df.Records())-1; i++ {
		if !df.Col("ID").Elem(i).IsNA() {
			index, err := df.Col("ID").Elem(i).Int()
			if err != nil {
				log.Fatal(err)
			}
			id = append(id, index)
		}
	}
	return id
}

func extractData(df dataframe.DataFrame, id int) []string {
	col := []string{}
	for i := 0; i < len(df.Records())-1; i++ {
		index, err := df.Col("ID").Elem(i).Int()
		if err != nil {
			log.Fatal(err)
		}
		if index == id {
			if df.Col("Technical Skills").Elem(i).String() != "" {
				value := df.Col("Technical Skills").Elem(i).String()
				col = append(col, value)
			}
		}
	}
	return col
}
