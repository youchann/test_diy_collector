package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Receivers struct {
		SmartAgentSQL struct {
			Type            string `yaml:"type"`
			IntervalSeconds int    `yaml:"intervalSeconds"`
			DBDriver        string `yaml:"dbDriver"`
			Params          struct {
				Account          string `yaml:"account"`
				Database         string `yaml:"database"`
				Warehouse        string `yaml:"warehouse"`
				User             string `yaml:"user"`
				Password         string `yaml:"password"`
				ConnectionString string `yaml:"connectionString"`
			} `yaml:"params"`
			Queries []struct {
				Query   string   `yaml:"query"`
				Metrics []Metric `yaml:"metrics"`
			} `yaml:"queries"`
		} `yaml:"smartagent/sql"`
	} `yaml:"receivers"`
}

type Metric struct {
	MetricName       string   `yaml:"metricName"`
	ValueColumn      string   `yaml:"valueColumn"`
	DimensionColumns []string `yaml:"dimensionColumns"`
}

func main() {
	// Read configuration from YAML file
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}

	// Construct the connection string
	params := config.Receivers.SmartAgentSQL.Params
	connStr := fmt.Sprintf("%s:%s@%s/%s?warehouse=%s",
		params.User, params.Password, params.Account, params.Database, params.Warehouse)

	// Connect to Snowflake
	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	// Execute queries and process results
	for _, queryConfig := range config.Receivers.SmartAgentSQL.Queries {
		rows, err := db.Query(queryConfig.Query)
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		var eventData []map[string]interface{}

		columns, err := rows.Columns()
		if err != nil {
			log.Fatalf("Failed to get columns: %v", err)
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))

			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Fatalf("Failed to scan row: %v", err)
			}

			data := make(map[string]interface{})
			for i, col := range columns {
				data[col] = values[i]
			}
			eventData = append(eventData, data)
		}

		if err := rows.Err(); err != nil {
			log.Fatalf("Row iteration error: %v", err)
		}

		sendToSplunk(eventData, queryConfig.Metrics)
	}

}

type Gauge struct {
	Metric     string                 `json:"metric"`
	Value      float64                `json:"value"`
	Dimensions map[string]interface{} `json:"dimensions"`
	Timestamp  int64                  `json:"timestamp"`
}

type Request struct {
	Gauge []Gauge `json:"gauge"`
}

func sendToSplunk(rows []map[string]interface{}, metrics []Metric) {
	var gauges []Gauge
	for _, metric := range metrics {
		for _, row := range rows {
			value, err := strconv.ParseFloat(getValue[string](row, metric.ValueColumn), 64)
			if err != nil {
				log.Fatalf("Failed to convert %s to float64: %v", metric.ValueColumn, err)
			}
			dimensions := make(map[string]interface{})
			for _, column := range metric.DimensionColumns {
				dimensions[column] = getValue[interface{}](row, column)
			}
			gauges = append(gauges, Gauge{
				Metric:     metric.MetricName,
				Value:      value,
				Dimensions: dimensions,
				Timestamp:  getValue[time.Time](row, "EVENT_AT").UnixMilli(),
			})
		}
	}

	splunkURL := "https://ingest.us1.signalfx.com/v2/datapoint"
	splunkToken := "xxxxxxxxx"

	r := Request{Gauge: gauges}
	eventJSON, err := json.Marshal(r)
	if err != nil {
		log.Fatalf("Failed to marshal event to JSON: %v", err)
	}

	req, err := http.NewRequest("POST", splunkURL, bytes.NewBuffer(eventJSON))
	if err != nil {
		log.Fatalf("Failed to create HTTP request: %v", err)
	}

	req.Header.Set("Authorization", "Splunk "+splunkToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()
	fmt.Println("Splunk response: ", resp.Status)
}

func getValue[T any](row map[string]interface{}, column string) T {
	value, exists := row[column]
	if !exists {
		log.Fatalf("Column %s does not exist", column)
	}

	typedValue, ok := value.(T)
	if !ok {
		log.Fatalf("Failed to convert %s to %T", column, typedValue)
	}

	return typedValue
}
