#!/usr/bin/env bash
#
# token-watch.sh — track per-token socket health over time.
#
# The sniper prints one line per token every few seconds:
#   HH:MM:SS.mmm [label] stats: seen N (X/min) taken M uptime Ym
# and a line on every drop:
#   HH:MM:SS.mmm [label] reconnect in ...
#
# This reads the sniper's log and appends a CSV row per token on each tick, so you
# can watch uptime climb, reconnect churn, and coverage rate. Point it at the file
# the sniper's stdout is redirected to.
#
# Usage:
#   ./p2c-sniper config.json > /var/log/p2c.log 2>&1 &
#   scripts/token-watch.sh /var/log/p2c.log [out.csv] [interval_sec]
#
# systemd (optional):
#   [Unit]
#   Description=P2C token watch
#   After=p2c-sniper.service
#   [Service]
#   ExecStart=/opt/p2c-sniper/scripts/token-watch.sh /var/log/p2c.log /var/log/p2c-tokens.csv 120
#   Restart=always
#   [Install]
#   WantedBy=multi-user.target
set -euo pipefail

LOG="${1:-${P2C_LOG:-}}"
OUT="${2:-token-aging.csv}"
INTERVAL="${3:-120}"

if [[ -z "$LOG" ]]; then
	echo "usage: $0 <sniper-log> [out.csv] [interval_sec]" >&2
	exit 1
fi

[[ -f "$OUT" ]] || echo "ts,label,uptime_min,reconnects,seen,taken,rate_per_min" >"$OUT"

while true; do
	[[ -f "$LOG" ]] || { sleep "$INTERVAL"; continue; }
	now=$(date +%s)

	# labels that have emitted a stats line
	labels=$(grep -oE '\[[^]]+\] stats:' "$LOG" 2>/dev/null | sed -E 's/^\[([^]]+)\] stats:/\1/' | sort -u)

	for label in $labels; do
		# last stats line for this label: "... stats: seen N (X/min) taken M uptime Ym"
		line=$(grep -F "[$label] stats:" "$LOG" | tail -1)
		seen=$(sed -nE 's/.*seen ([0-9]+).*/\1/p' <<<"$line")
		rate=$(sed -nE 's/.*\(([0-9.]+)\/min\).*/\1/p' <<<"$line")
		taken=$(sed -nE 's/.*taken ([0-9]+).*/\1/p' <<<"$line")
		uptime=$(sed -nE 's/.*uptime ([0-9.]+)m.*/\1/p' <<<"$line")
		reconn=$(grep -cF "[$label] reconnect in" "$LOG" || true)

		echo "$now,$label,${uptime:-0},${reconn:-0},${seen:-0},${taken:-0},${rate:-0}" >>"$OUT"
	done

	sleep "$INTERVAL"
done
