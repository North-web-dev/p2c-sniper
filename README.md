# p2c-sniper

A latency-focused order sniper for the CryptoBot P2C section (`app.cr.bot` /
`app.send.tg`). It watches the live order feed over WebSocket and races to take
matching orders before other operators.

## How it works

Orders are distributed FIFO over a Socket.IO feed — the oldest connection
receives each order first. The sniper:

1. Holds one long-lived WebSocket per token and reconnects instantly on drop, so
   it keeps its position near the front of the queue.
2. On a `list:update` `op:add` frame, checks the RUB amount against the filter.
3. Fires the take over a pre-warmed HTTP/2 channel, **hedged** — `take_shots`
   parallel `POST take/{id}` requests, the first `200` wins and the rest are
   dropped. Hedging collapses the tail latency of the take.

Multiple tokens run concurrently (each with its own proxy), which increases the
number of queue positions held.

## Build

```sh
go build -o p2c-sniper .
```

Requires Go 1.21+.

## Config

Copy `config.example.json` to `config.json` and fill it in:

| field                  | meaning                                                        |
|------------------------|---------------------------------------------------------------|
| `host`                 | API host (default `app.cr.bot`)                               |
| `min_rub` / `max_rub`  | take only orders inside this RUB range                        |
| `take_shots`           | parallel take requests per order (8 is a good default)        |
| `warmer_interval_sec`  | how often to poll to keep the HTTP channel warm               |
| `reconnect_min_ms/max` | reconnect delay window (jittered)                             |
| `telegram_bot_token`   | optional — notifications + inline order controls              |
| `telegram_chat_id`     | admin chat id that may use the controls                       |
| `accounts[]`           | `label`, `token` (access_token cookie), `proxy`, `payment_method_id` |

`payment_method_id` is resolved automatically from `/p2c/accounts` when left empty.

## Run

```sh
./p2c-sniper config.json
```

Optionally as a service:

```ini
# /etc/systemd/system/p2c-sniper.service
[Unit]
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/opt/p2c-sniper
ExecStart=/opt/p2c-sniper/p2c-sniper /opt/p2c-sniper/config.json
Restart=always
RestartSec=2
LimitNOFILE=1048576
Nice=-10

[Install]
WantedBy=multi-user.target
```

Set `insecure_tls: true` only when running behind a TLS-terminating (MITM) proxy.
Set `P2C_DEBUG=1` to log every order seen on the feed (noisy; off by default).

## Telegram controls

When a token takes an order the bot posts a notification with inline buttons —
**Complete / Cancel / Dispute / Refund** — which run the corresponding
`/p2c/payments/{id}/{action}` call through the same token that took the order.
Only the configured `telegram_chat_id` is allowed to use them.

## Operations

Order distribution is FIFO and coverage scales with the number of tokens, not with
socket age — see [TOKENS.md](TOKENS.md) for the holding strategy.

`scripts/token-watch.sh` tails the sniper's log and writes a per-token CSV
(uptime, reconnects, seen, rate) so you can watch socket health over time:

```sh
./p2c-sniper config.json > /var/log/p2c.log 2>&1 &
scripts/token-watch.sh /var/log/p2c.log token-aging.csv 120
```
