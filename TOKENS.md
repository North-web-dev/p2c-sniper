# Token holding

Notes on how orders are distributed and what actually moves the hit rate.
Numbers below come from ~20h of continuous observation against the live feed.

## Model

Orders are broadcast FIFO over the Socket.IO feed. Broadcast order follows join
order (oldest connection first), and the per-order fan-out is small — roughly the
**~4 oldest sockets** receive each new `list:update` `op:add`. So the closer a
socket is to the front of the queue, the more orders it sees and the earlier it
sees them.

One token = one long-lived, aged socket that holds a queue position.

## What actually helps

- **Token count, not socket age.** Aging a single socket does *not* increase its
  coverage: a 40-minute-old socket and a 2.6-hour-old socket both saw ~2 orders/min.
  Coverage scales with the number of **distinct tokens**, because each token is a
  separate queue position competing for the ~4-wide fan-out.
- **Multiple sockets on one token are useless.** The per-token cap is ~4-5 live
  sockets; opening more triggers dedup kicks (`41` on the feed). Worse, every socket
  on the same token receives the *same* orders — unique coverage does not grow. Only
  additional tokens add coverage.

## Holding a socket

- **Reconnect instantly on drop** (no sleep beyond a few ms of jitter) to reclaim the
  queue position. A dropped socket rejoins at the back, so downtime costs position.
- **Sockets live ~4h max**, then get torn (server-side churn or infra). This is the
  ceiling — expect a reconnect every few hours per socket and design for it.
- **Hold gently.** A clean, lightly-used token is stable (~4 reconnects over 20h). A
  token hammered with take attempts gets destabilized by the server (~222 reconnects
  over the same window on an abused token). Spraying takes also trips `44 RateLimited`.

## Practical guidance

- Run one socket per token; scale by adding tokens, not sockets or age.
- Keep `take_shots` modest — hedge a genuine target order, don't spray the endpoint.
- Filter tightly (`min_rub`/`max_rub`) so takes only fire on orders you actually want,
  which keeps each token's take rate low and its socket stable.
