// Telegram control bot: long-polls getUpdates and handles the inline buttons
// attached to a "taken order" notification. callback_data is "<action>:<orderID>";
// the action is executed through the worker that took the order (via orderRegistry).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type tgBot struct {
	token  string
	chat   int64
	client *http.Client
}

func newTGBot(token string, chat int64) *tgBot {
	if token == "" || chat == 0 {
		return nil
	}
	return &tgBot{token: token, chat: chat,
		client: &http.Client{Timeout: 70 * time.Second}}
}

func (b *tgBot) api(method string, payload any) ([]byte, error) {
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", "https://api.telegram.org/bot"+b.token+"/"+method,
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

type tgUpdate struct {
	UpdateID      int64 `json:"update_id"`
	CallbackQuery *struct {
		ID   string `json:"id"`
		From struct {
			ID int64 `json:"id"`
		} `json:"from"`
		Data string `json:"data"`
	} `json:"callback_query"`
}

func (b *tgBot) answer(cbID, text string) {
	b.api("answerCallbackQuery", map[string]any{
		"callback_query_id": cbID, "text": text, "show_alert": false})
}

// run is the long-poll loop; it only reacts to the configured admin chat.
func (b *tgBot) run(ctx context.Context) {
	var offset int64
	logf("tg", "control bot started (long-poll), chat=%d", b.chat)
	for {
		if ctx.Err() != nil {
			return
		}
		raw, err := b.api("getUpdates", map[string]any{
			"offset": offset, "timeout": 50,
			"allowed_updates": []string{"callback_query"}})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		var out struct {
			OK     bool       `json:"ok"`
			Result []tgUpdate `json:"result"`
		}
		if json.Unmarshal(raw, &out) != nil || !out.OK {
			continue
		}
		for _, u := range out.Result {
			offset = u.UpdateID + 1
			cq := u.CallbackQuery
			if cq == nil {
				continue
			}
			if cq.From.ID != b.chat {
				b.answer(cq.ID, "denied")
				continue
			}
			b.dispatch(cq.ID, cq.Data)
		}
	}
}

func (b *tgBot) dispatch(cbID, data string) {
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 {
		b.answer(cbID, "?")
		return
	}
	action, orderID := parts[0], parts[1]
	v, ok := orderRegistry.Load(orderID)
	if !ok {
		b.answer(cbID, "order not in registry")
		return
	}
	w := v.(*worker)
	switch action {
	case "complete", "cancel", "dispute", "refund":
		st, body := w.paymentAction(orderID, action)
		if st == 200 {
			b.answer(cbID, action+" ok")
		} else {
			b.answer(cbID, action+" "+strconv.Itoa(st)+" "+trunc(body, 60))
		}
	default:
		b.answer(cbID, "unknown action")
	}
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
