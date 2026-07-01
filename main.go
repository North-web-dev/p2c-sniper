// P2C order sniper for CryptoBot (app.cr.bot / app.send.tg).
//
// Orders are handed out FIFO over a Socket.IO feed — the oldest connection sees
// each order first. Strategy: hold one long-lived socket per token, reconnect
// instantly on drop, and fire the take over a pre-warmed HTTP/2 channel. Each
// take is hedged (N parallel requests, first 200 wins) to cut tail latency.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type Account struct {
	Label           string `json:"label"`
	Token           string `json:"token"`
	Proxy           string `json:"proxy"`
	PaymentMethodID string `json:"payment_method_id"`
}

type Config struct {
	Host              string    `json:"host"`
	MinRUB            float64   `json:"min_rub"`
	MaxRUB            float64   `json:"max_rub"`
	TakeShots         int       `json:"take_shots"`
	WarmerIntervalSec int       `json:"warmer_interval_sec"`
	ReconnectMinMs    int       `json:"reconnect_min_ms"`
	ReconnectMaxMs    int       `json:"reconnect_max_ms"`
	TelegramBotToken  string    `json:"telegram_bot_token"`
	TelegramChatID    int64     `json:"telegram_chat_id"`
	InsecureTLS       bool      `json:"insecure_tls"` // skip cert verification (needed behind a MITM proxy)
	Accounts          []Account `json:"accounts"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Host == "" {
		c.Host = "app.cr.bot"
	}
	if c.TakeShots <= 0 {
		c.TakeShots = 8
	}
	if c.WarmerIntervalSec <= 0 {
		c.WarmerIntervalSec = 5
	}
	if c.ReconnectMinMs <= 0 {
		c.ReconnectMinMs = 50
	}
	if c.ReconnectMaxMs <= c.ReconnectMinMs {
		c.ReconnectMaxMs = c.ReconnectMinMs + 200
	}
	if c.MaxRUB <= 0 {
		c.MaxRUB = math.MaxFloat64
	}
	return &c, nil
}

var wei = new(big.Float).SetFloat64(1e18)

// debugLog gates the per-order "seen" line, which is noisy at feed scale.
var debugLog = os.Getenv("P2C_DEBUG") != ""

// weiToRUB converts an amount that may arrive as a wei-scaled (x1e18) string,
// float, or json.Number into a plain RUB value.
func weiToRUB(v any) float64 {
	var s string
	switch t := v.(type) {
	case string:
		s = t
	case float64:
		return t / 1e18 // large ints lose precision as float, fine for a threshold
	case json.Number:
		s = t.String()
	default:
		return 0
	}
	f, ok := new(big.Float).SetString(s)
	if !ok {
		if x, e := strconv.ParseFloat(s, 64); e == nil {
			return x
		}
		return 0
	}
	r, _ := new(big.Float).Quo(f, wei).Float64()
	return r
}

func ts() string { return time.Now().Format("15:04:05.000") }

func logf(label, format string, a ...any) {
	fmt.Printf("%s [%s] %s\n", ts(), label, fmt.Sprintf(format, a...))
}

type notifier struct {
	token  string
	chat   int64
	client *http.Client
}

func (n *notifier) send(text string) { n.sendMarkup(text, nil) }

func (n *notifier) sendMarkup(text string, markup any) {
	if n == nil || n.token == "" || n.chat == 0 {
		return
	}
	go func() {
		m := map[string]any{
			"chat_id": n.chat, "text": text, "parse_mode": "HTML",
			"disable_web_page_preview": true,
		}
		if markup != nil {
			m["reply_markup"] = markup
		}
		body, _ := json.Marshal(m)
		req, _ := http.NewRequest("POST",
			"https://api.telegram.org/bot"+n.token+"/sendMessage",
			bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := n.client.Do(req)
		if err != nil {
			logf("tg", "notify send failed: %v", err)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
}

func takeKeyboard(orderID string) map[string]any {
	btn := func(text, action string) map[string]string {
		return map[string]string{"text": text, "callback_data": action + ":" + orderID}
	}
	return map[string]any{"inline_keyboard": [][]map[string]string{
		{btn("Complete", "complete"), btn("Cancel", "cancel")},
		{btn("Dispute", "dispute"), btn("Refund", "refund")},
	}}
}

// orderRegistry maps orderID -> *worker so a Telegram callback acts through the
// same token that took the order.
var orderRegistry sync.Map

type worker struct {
	cfg   *Config
	acc   Account
	notif *notifier

	host    string
	reqBody []byte // {"payment_method_id":"<id>"}, marshalled once
	reqID   string

	httpc  *http.Client
	dialer *websocket.Dialer

	seen    sync.Map // order_id -> struct{}, guards against double-take in-process
	took    int64
	seenCnt int64
	dead    atomic.Bool
}

func newWorker(cfg *Config, acc Account, n *notifier) (*worker, error) {
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.InsecureTLS},
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	wd := &websocket.Dialer{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: cfg.InsecureTLS},
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: false,
	}
	if acc.Proxy != "" {
		pu, err := url.Parse(acc.Proxy)
		if err != nil {
			return nil, fmt.Errorf("bad proxy %q: %w", acc.Proxy, err)
		}
		tr.Proxy = http.ProxyURL(pu)
		wd.Proxy = http.ProxyURL(pu)
	}
	return &worker{
		cfg:    cfg,
		acc:    acc,
		notif:  n,
		host:   cfg.Host,
		reqID:  acc.PaymentMethodID,
		httpc:  &http.Client{Transport: tr, Timeout: 6 * time.Second},
		dialer: wd,
	}, nil
}

func (w *worker) base() string { return "https://" + w.host + "/internal/v1" }

func (w *worker) httpHeaders() http.Header {
	h := http.Header{}
	h.Set("Cookie", "access_token="+w.acc.Token)
	h.Set("Origin", "https://"+w.host)
	h.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "application/json, text/plain, */*")
	return h
}

// resolveReqID fetches payment_method_id from /p2c/accounts when not configured.
func (w *worker) resolveReqID(ctx context.Context) error {
	if w.reqID != "" {
		w.reqBody, _ = json.Marshal(map[string]string{"payment_method_id": w.reqID})
		return nil
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", w.base()+"/p2c/accounts", nil)
	req.Header = w.httpHeaders()
	resp, err := w.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 {
		return fmt.Errorf("401: token dead")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("accounts http %d", resp.StatusCode)
	}
	var out struct {
		Data []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
			Bank  string `json:"bank_code"`
		} `json:"data"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	if len(out.Data) == 0 {
		return fmt.Errorf("no payment methods on account")
	}
	w.reqID = out.Data[0].ID
	w.reqBody, _ = json.Marshal(map[string]string{"payment_method_id": w.reqID})
	logf(w.acc.Label, "payment method: %s (%s %s)", w.reqID, out.Data[0].Title, out.Data[0].Bank)
	return nil
}

// warmer keeps the HTTP channel hot and detects a dead token via 401.
func (w *worker) warmer(ctx context.Context) {
	t := time.NewTicker(time.Duration(w.cfg.WarmerIntervalSec) * time.Second)
	defer t.Stop()
	url := w.base() + "/p2c/payments?size=20"
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if w.dead.Load() {
				return
			}
			req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
			req.Header = w.httpHeaders()
			resp, err := w.httpc.Do(req)
			if err != nil {
				continue
			}
			if resp.StatusCode == 401 {
				resp.Body.Close()
				w.markDead("warmer 401")
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

func (w *worker) statsLoop(ctx context.Context) {
	start := time.Now()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if w.dead.Load() {
				return
			}
			el := time.Since(start).Seconds()
			sc := atomic.LoadInt64(&w.seenCnt)
			tk := atomic.LoadInt64(&w.took)
			logf(w.acc.Label, "stats: seen %d (%.0f/min) taken %d uptime %.0fm",
				sc, float64(sc)/(el/60), tk, el/60)
		}
	}
}

func (w *worker) markDead(reason string) {
	if w.dead.Swap(true) {
		return
	}
	logf(w.acc.Label, "token dead: %s", reason)
	w.notif.send(fmt.Sprintf("<b>%s</b>: token dead (%s)", w.acc.Label, reason))
}

// takeOrder fires TakeShots parallel POSTs; the first 200 wins, the rest are
// dropped. The per-process seen map prevents a duplicate take of the same order.
func (w *worker) takeOrder(o orderInfo) {
	if _, dup := w.seen.LoadOrStore(o.id, struct{}{}); dup {
		return
	}
	url := w.base() + "/p2c/payments/take/" + o.id
	var winner atomic.Bool
	var wg sync.WaitGroup
	detected := time.Now()
	for i := 0; i < w.cfg.TakeShots; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("POST", url, bytes.NewReader(w.reqBody))
			req.Header = w.httpHeaders()
			start := time.Now()
			resp, err := w.httpc.Do(req)
			if err != nil {
				return
			}
			lat := time.Since(start).Milliseconds()
			st := resp.StatusCode
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			resp.Body.Close()
			switch st {
			case 200:
				if !winner.Swap(true) {
					atomic.AddInt64(&w.took, 1)
					orderRegistry.Store(o.id, w)
					logf(w.acc.Label, "TAKEN %.0fRUB | %s | id=%s | %dms (detect->take %dms)",
						o.rub, o.brand, o.id, lat, time.Since(detected).Milliseconds())
					link := ""
					if o.url != "" {
						link = "\n" + o.url
					}
					if o.payload != "" {
						link += "\n<code>" + o.payload + "</code>"
					}
					msg := fmt.Sprintf("<b>TAKEN %.0fRUB</b> — %s\nid <code>%s</code>\n%dms · %s%s",
						o.rub, o.brand, o.id, lat, w.acc.Label, link)
					w.notif.sendMarkup(msg, takeKeyboard(o.id))
				}
			case 401:
				w.markDead("take 401")
			default:
				if !winner.Load() {
					logf(w.acc.Label, "miss %.0fRUB | %d | %s", o.rub, st, strings.TrimSpace(string(b)))
				}
			}
		}()
	}
	wg.Wait()
}

// paymentAction runs complete/cancel/dispute/refund on a taken order.
func (w *worker) paymentAction(orderID, action string) (int, string) {
	url := w.base() + "/p2c/payments/" + orderID + "/" + action
	req, _ := http.NewRequest("POST", url, bytes.NewReader([]byte("{}")))
	req.Header = w.httpHeaders()
	resp, err := w.httpc.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode == 401 {
		w.markDead(action + " 401")
	}
	logf(w.acc.Label, "%s id=%s -> %d", action, orderID, resp.StatusCode)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

type orderInfo struct {
	id, brand, url, payload string
	rub                     float64
}

// handleFrame implements the Engine.IO v4 / Socket.IO handshake and returns the
// frame to send back ("" for none):
//
//	"0{...}"  open       -> "40"                   (Socket.IO CONNECT, namespace "/")
//	"40{...}" connect ok -> 42["list:initialize"]  (subscribe to the order feed)
//	"2"       ping       -> "3"                     (pong)
//	"42[...]" event      -> parse orders on op:add
func (w *worker) handleFrame(p []byte) string {
	switch {
	case len(p) == 1 && p[0] == '2':
		return "3"
	case len(p) >= 1 && p[0] == '0':
		return "40"
	case len(p) >= 2 && p[0] == '4' && p[1] == '0':
		return `42["list:initialize"]`
	case len(p) >= 2 && p[0] == '4' && p[1] == '2':
		if bytes.Contains(p, []byte(`"op":"add"`)) {
			w.parseOrders(p)
		}
		return ""
	default:
		return ""
	}
}

func (w *worker) parseOrders(p []byte) {
	idx := bytes.IndexByte(p, '[')
	if idx < 0 {
		return
	}
	var ev []json.RawMessage
	if err := json.Unmarshal(p[idx:], &ev); err != nil || len(ev) < 2 {
		return
	}
	var items []struct {
		Op   string `json:"op"`
		Data struct {
			ID        string `json:"id"`
			InAsset   string `json:"in_asset"`
			InAmount  string `json:"in_amount"`  // plain RUB string when in_asset=RUB
			OutAmount string `json:"out_amount"` // crypto in wei
			BrandName string `json:"brand_name"`
			URL       string `json:"url"`
			Payload   string `json:"payload"`
			Provider  string `json:"provider"`
			MCC       string `json:"mcc"`
			Boost     int    `json:"boost"`
		} `json:"data"`
	}
	if err := json.Unmarshal(ev[1], &items); err != nil {
		return
	}
	for _, it := range items {
		if it.Op != "add" || it.Data.ID == "" {
			continue
		}
		atomic.AddInt64(&w.seenCnt, 1)
		d := it.Data
		rub, _ := strconv.ParseFloat(strings.TrimSpace(d.InAmount), 64)
		if !strings.EqualFold(d.InAsset, "RUB") && rub == 0 {
			rub = weiToRUB(d.OutAmount)
		}
		hit := rub >= w.cfg.MinRUB && rub <= w.cfg.MaxRUB
		if debugLog {
			tag := ""
			if hit {
				tag = " TARGET"
			}
			logf(w.acc.Label, "seen %s | %.0fRUB | %s | mcc=%s boost=%d%s",
				d.ID, rub, d.BrandName, d.MCC, d.Boost, tag)
		}
		if hit {
			go w.takeOrder(orderInfo{id: d.ID, rub: rub, brand: d.BrandName, url: d.URL, payload: d.Payload})
		}
	}
}

// runWS runs one socket lifetime and returns on disconnect.
func (w *worker) runWS(ctx context.Context) error {
	u := fmt.Sprintf("wss://%s/internal/v1/p2c-socket/?EIO=4&transport=websocket", w.host)
	c, _, err := w.dialer.DialContext(ctx, u, w.httpHeaders())
	if err != nil {
		return err
	}
	defer c.Close()
	logf(w.acc.Label, "socket connected")

	// Server pings "2" every ~25s with a 20s pong timeout; refresh the read
	// deadline on every frame.
	for {
		if w.dead.Load() {
			return nil
		}
		c.SetReadDeadline(time.Now().Add(50 * time.Second))
		_, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if reply := w.handleFrame(msg); reply != "" {
			c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.WriteMessage(websocket.TextMessage, []byte(reply)); err != nil {
				return err
			}
		}
	}
}

func (w *worker) run(ctx context.Context) {
	if err := w.resolveReqID(ctx); err != nil {
		w.markDead("resolveReqID: " + err.Error())
		return
	}
	go w.warmer(ctx)
	go w.statsLoop(ctx)
	logf(w.acc.Label, "start | host=%s filter=[%.0f..%.0f]RUB shots=%d proxy=%v",
		w.host, w.cfg.MinRUB, w.cfg.MaxRUB, w.cfg.TakeShots, w.acc.Proxy != "")

	span := w.cfg.ReconnectMaxMs - w.cfg.ReconnectMinMs
	for {
		if ctx.Err() != nil || w.dead.Load() {
			return
		}
		err := w.runWS(ctx)
		if w.dead.Load() || ctx.Err() != nil {
			return
		}
		// Reconnect fast with a little jitter to reclaim queue position.
		d := time.Duration(w.cfg.ReconnectMinMs+rand.Intn(maxInt(span, 1))) * time.Millisecond
		logf(w.acc.Label, "reconnect in %v (%v)", d, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	debug.SetGCPercent(400) // fewer GC pauses on the hot path

	cfgPath := "config.json"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		fmt.Println("config:", err)
		os.Exit(1)
	}
	if len(cfg.Accounts) == 0 {
		fmt.Println("no accounts in config")
		os.Exit(1)
	}

	var n *notifier
	var tg *tgBot
	if cfg.TelegramBotToken != "" && cfg.TelegramChatID != 0 {
		n = &notifier{token: cfg.TelegramBotToken, chat: cfg.TelegramChatID,
			client: &http.Client{Timeout: 10 * time.Second}}
		tg = newTGBot(cfg.TelegramBotToken, cfg.TelegramChatID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if tg != nil {
		go tg.run(ctx)
	}

	logf("main", "starting: %d account(s), host=%s", len(cfg.Accounts), cfg.Host)
	var wg sync.WaitGroup
	for _, acc := range cfg.Accounts {
		if acc.Token == "" || strings.Contains(acc.Token, "PASTE_") {
			logf("main", "skip %s: token not set", acc.Label)
			continue
		}
		w, err := newWorker(cfg, acc, n)
		if err != nil {
			logf("main", "skip %s: %v", acc.Label, err)
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); w.run(ctx) }()
	}
	wg.Wait()
	logf("main", "stopped")
}
