package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var (
	postgresDSN = os.Getenv("POSTGRES_DSN") 
	pairsOverride = os.Getenv("PAIRS_OVERRIDE") // e.g. "BTCUSDT,ETHUSDT" 
	debugEnabled = os.Getenv("DEBUG") == "1"
)
const (
	FLUSH_INTERVAL = 5 * time.Second
	RETENTION_H    = 720 // hours
)

type CombinedTradeMsg struct {
	Stream string `json:"stream"`
	Data   struct {
		Price        string `json:"p"`
		Qty          string `json:"q"`
		IsBuyerMaker bool   `json:"m"` // ← THIS is the one we want
		Ignore       bool   `json:"M"` // <-- explicitly absorb it
		EventTime    int64  `json:"T"`
	} `json:"data"`
}

type Flow struct {
	Buy  float64
	Sell float64
	Price float64
	LastEvt time.Time
}

var (
	flows = map[string]*Flow{}
	mu    sync.Mutex
)

func main() {
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ensureTable(db)

	pairs := loadPairs()
	if len(pairs) == 0 {
		log.Fatal("No pairs provided (PAIRS_OVERRIDE empty and default list empty)")
	}

	mu.Lock()
	for _, p := range pairs {
		flows[p] = &Flow{}
	}
	mu.Unlock()

	go startCombinedWS(pairs)

	ticker := time.NewTicker(FLUSH_INTERVAL)
	defer ticker.Stop()

	for range ticker.C {
		flush(db)
		cleanup(db)
	}
}

func ensureTable(db *sql.DB) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS trade_flow (
		event_ts TIMESTAMPTZ NOT NULL, 
		recv_ts TIMESTAMPTZ NOT NULL,
		pair TEXT NOT NULL,
		buy_vol DOUBLE PRECISION,
		sell_vol DOUBLE PRECISION,
		delta DOUBLE PRECISION,
		price DOUBLE PRECISION
	);

	CREATE INDEX IF NOT EXISTS idx_trade_flow_event ON trade_flow(event_ts);
	CREATE INDEX IF NOT EXISTS idx_trade_flow_pair ON trade_flow(pair);
	`)
	if err != nil {
		log.Fatal("ensureTable error:", err)
	}
}

// --------------------
// WebSocket (combined)
// --------------------

func startCombinedWS(pairs []string) {
	var streams []string
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@aggTrade")
	}

	wsURL := "wss://stream.binance.com:9443/stream?streams=" + strings.Join(streams, "/")

	for {
		log.Println("🔌 Connecting to:", wsURL)

		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Println("❌ WS dial error:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Println("🟢 WebSocket connected")

		// --- Heartbeat setup ---
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))

		ws.SetPongHandler(func(string) error {
			ws.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		// Ping every 30s
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				if err := ws.WriteControl(
					websocket.PingMessage,
					[]byte("ping"),
					time.Now().Add(5*time.Second),
				); err != nil {
					log.Println("⚠️ Ping failed:", err)
					_ = ws.Close()
					return
				}
			}
		}()

		// --- Read loop ---
		for {
			_, msg, err := ws.ReadMessage()
			if err != nil {
				log.Println("🔴 WS read error:", err)
				_ = ws.Close()
				break
			}

			debugLog("RAW: %s", string(msg))

			var t CombinedTradeMsg
			if err := json.Unmarshal(msg, &t); err != nil {
				continue
			}

			pair := strings.ToUpper(strings.Split(t.Stream, "@")[0])
			qty := parseFloat(t.Data.Qty)
			price := parseFloat(t.Data.Price)
			eventTime := time.UnixMilli(t.Data.EventTime)

			mu.Lock()
			if f, ok := flows[pair]; ok {
				if t.Data.IsBuyerMaker {
					f.Sell += qty
				} else {
					f.Buy += qty
				}
				f.Price = price
				f.LastEvt = eventTime
			}
			mu.Unlock()
		}

		log.Println("🔁 Reconnecting in 2s...")
		time.Sleep(2 * time.Second)
	}
}


// --------------------
// DB flush / cleanup
// --------------------

func flush(db *sql.DB) {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now().UTC()

	for pair, f := range flows {
		delta := f.Buy - f.Sell

		eventTS := f.LastEvt
		if eventTS.IsZero() {
			eventTS = now
		}

		debugLog(
			"FLUSH %s | buy=%.6f sell=%.6f delta=%.6f price=%.2f",
			pair, f.Buy, f.Sell, delta, f.Price,
		)

		if f.Buy == 0 && f.Sell == 0 { continue }

		_, err := db.Exec(`
			INSERT INTO trade_flow (event_ts, recv_ts, pair, buy_vol, sell_vol, delta, price)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, eventTS, now, pair, f.Buy, f.Sell, delta, f.Price)

		if err != nil {
			log.Printf("[%s] DB insert error: %v", pair, err)
		}

		// reset counters for next bucket
		f.Buy = 0
		f.Sell = 0
	}
}

func cleanup(db *sql.DB) {
	// Keep table lean (48h)
	_, err := db.Exec(`
		DELETE FROM trade_flow
		WHERE event_ts < NOW() - INTERVAL '` + strconv.Itoa(RETENTION_H) + ` hours'
	`)
	if err != nil {
		log.Println("Cleanup error:", err)
	}
}

// --------------------
// Helpers
// --------------------

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func loadPairs() []string {
	// Override: "BTCUSDT,ETHUSDT"
	if strings.TrimSpace(pairsOverride) != "" {
		raw := strings.Split(pairsOverride, ",")
		var out []string
		for _, p := range raw {
			p = strings.TrimSpace(strings.ToUpper(p))
			p = strings.ReplaceAll(p, "/", "")
			if p != "" {
				out = append(out, p)
			}
		}
		log.Printf("🔒 Using PAIRS_OVERRIDE (%d pairs): %v", len(out), out)
		return out
	}

	// Default (your use-case)
	out := []string{"BTCUSDT", "ETHUSDT"}
	log.Printf("📌 Using default pairs: %v", out)
	return out
}

func debugLog(format string, args ...interface{}) {
	if debugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}