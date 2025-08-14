package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type RateLimiter struct {
	mu          sync.Mutex
	count       int
	limitPerMin int
	resetTime   time.Time
}

func NewRateLimiter(limit int) *RateLimiter {
	return &RateLimiter{
		limitPerMin: limit,
		resetTime:   time.Now().Add(61 * time.Second),
	}
}

func (rl *RateLimiter) Wait() {
	for {
		rl.mu.Lock()

		now := time.Now()
		if now.After(rl.resetTime) {
			fmt.Printf("--- Request count reset. Previous minute's count: %d ---\n", rl.count)
			rl.count = 0
			rl.resetTime = now.Add(61 * time.Second)
		}

		if rl.count < rl.limitPerMin {
			rl.count++
			fmt.Printf("Request permitted. Current minute's count: %d/%d\n", rl.count, rl.limitPerMin)
			rl.mu.Unlock()
			return
		}

		sleepDuration := rl.resetTime.Sub(now)

		rl.mu.Unlock()

		if sleepDuration > 0 {
			fmt.Printf("Rate limit reached. Waiting for %v...\n", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}
}

type AggTrade struct {
	TradeId   int64  `json:"a"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	FirstId   int64  `json:"f"`
	LastId    int64  `json:"l"`
	Timestamp int64  `json:"T"`
	IsMaker   bool   `json:"m"`
	IsBest    bool   `json:"M"`
}

const (
	apiURL       = "https://api.binance.com/api/v3/aggTrades"
	limitPerReq  = 1000
	maxReqPerMin = 1499 // 6000 (총 가중치) / 4 (요청당 가중치)
)

func fetchTrades(symbol string, fromId int64) ([]AggTrade, error) {
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("symbol", symbol)
	q.Add("limit", strconv.Itoa(limitPerReq))
	q.Add("fromId", strconv.FormatInt(fromId, 10))
	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: status code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var trades []AggTrade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		return nil, err
	}

	return trades, nil
}

func processSymbol(symbol string, rl *RateLimiter) {
	fmt.Printf("Starting data collection for %s...\n", symbol)
	if err := os.MkdirAll(symbol, os.ModePerm); err != nil {
		fmt.Printf("Error creating directory for %s: %v\n", symbol, err)
		return
	}

	var fromId int64 = 0

	for {
		rl.Wait()

		fmt.Printf("sym(%s) fromId(%d)\n", symbol, fromId)

		trades, err := fetchTrades(symbol, fromId)
		if err != nil {
			fmt.Printf("Error fetching trades for %s: %v\n", symbol, err)
			time.Sleep(5 * time.Second) // 에러 발생 시 대기
			continue
		}

		if len(trades) == 0 {
			fmt.Printf("No more trades found for %s. Finished.\n", symbol)
			break
		}

		groupedTrades := groupTradesByDate(trades)
		for date, records := range groupedTrades {
			filePath := fmt.Sprintf("%s/%s.csv", symbol, date)
			if err := saveToCSV(filePath, records); err != nil {
				fmt.Printf("Error saving to CSV for %s on %s: %v\n", symbol, date, err)
			}
		}

		lastTrade := trades[len(trades)-1]
		fromId = lastTrade.TradeId + 1
	}
}

func groupTradesByDate(trades []AggTrade) map[string][][]string {
	grouped := make(map[string][][]string)
	for _, trade := range trades {
		//t := time.UnixMilli(trade.Timestamp).In(time.FixedZone("KST", 9*60*60))
		t := time.UnixMilli(trade.Timestamp).UTC()
		dateStr := t.Format("2006-01-02")
		record := []string{
			strconv.FormatInt(trade.TradeId, 10),
			trade.Price,
			trade.Quantity,
			strconv.FormatInt(trade.Timestamp, 10),
			strconv.FormatBool(trade.IsMaker),
		}
		grouped[dateStr] = append(grouped[dateStr], record)
	}
	return grouped
}

func saveToCSV(filePath string, records [][]string) error {
	_, err := os.Stat(filePath)
	isNewFile := os.IsNotExist(err)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if isNewFile {
		header := []string{"tradeId", "price", "quantity", "timestamp", "isBuyerMaker"}
		if err := writer.Write(header); err != nil {
			return err
		}
	}
	return writer.WriteAll(records)
}

func main() {
	rateLimiter := NewRateLimiter(maxReqPerMin)

	symbols := []string{"ETHUSDC", "ETHUSDT", "ETHBTC"}
	var wg sync.WaitGroup

	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			processSymbol(sym, rateLimiter)
		}(symbol)
	}

	wg.Wait()
	fmt.Println("All data collection tasks finished.")
}
