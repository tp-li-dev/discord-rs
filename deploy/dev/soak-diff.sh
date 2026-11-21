#!/usr/bin/env bash
# soak-report.sh を繰り返し実行したログ（tee -a soak.log）の、最初と最後のスナップショットを比べる。
# 読み取り専用。秘密は扱わない。使い方: soak-diff.sh [LOG=/home/ubuntu/soak.log] [SINCE=2026-11-16T12:00]
# SINCE を指定すると、その時刻（UTC。ISO 形式の同じ書式どうしの文字列比較）以降のスナップショットだけを比べる。
# 更新（botd の入れ替え）をまたぐと pid の変化がすべて WATCH になるので、更新の後だけを見たいときに使う。
# 役割（botd / core / gateway）ごとに pid の昇順で「役割#番号」を振る。pid が変われば再起動として WATCH。
# Core が複数あると再起動で番号が入れ替わり得る。soak-report.sh は cmd を 90 文字で切るため、
# --core-listen / --core-dir が切れても、Gateway は --shard-id（Core には無い）でも判定する。
set -u
LOG=${1:-/home/ubuntu/soak.log}
if [ ! -r "$LOG" ]; then echo "cannot read: $LOG" >&2; exit 2; fi
T=$(mktemp -d) || exit 2
trap 'rm -rf "$T"' EXIT
SINCE=${2:-}
if [ -n "$SINCE" ]; then
  # ヘッダの時刻（"=== soak snapshot 2026-11-16T12:00:01+00:00 ..."）が SINCE 以上のものから後を残す。
  awk -v S="$SINCE" '/^=== soak snapshot /{ keep = ($4 >= S) } keep { print }' "$LOG" > "$T/since.log"
  LOG="$T/since.log"
  if [ ! -s "$LOG" ]; then echo "no snapshots at or after $SINCE" >&2; exit 1; fi
fi
V="$T/verdict"; : > "$V"
add() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$V"; }
isnum() { case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }

N=$(grep -c '^=== soak snapshot ' "$LOG")
if [ "$N" -eq 0 ]; then echo "no snapshots in $LOG" >&2; exit 1; fi
FIRST_T=$(grep -m1 '^=== soak snapshot ' "$LOG" | awk '{print $4}')
LAST_T=$(grep '^=== soak snapshot ' "$LOG" | tail -n 1 | awk '{print $4}')
echo "log: ${1:-/home/ubuntu/soak.log}${SINCE:+ (since $SINCE)}"; echo "snapshots: $N"; echo "span: $FIRST_T .. $LAST_T"
if [ "$N" -lt 2 ]; then echo "only one snapshot: nothing to compare yet"; exit 0; fi
e1=$(date -d "$FIRST_T" +%s 2>/dev/null); e2=$(date -d "$LAST_T" +%s 2>/dev/null)
HOURS=$(awk -v a="${e1:-0}" -v b="${e2:-0}" 'BEGIN { if (a > 0 && b > a) printf "%.4f", (b - a) / 3600; else printf "0" }')
if [ "$HOURS" = "0" ]; then echo "elapsed: unknown (per-hour values n/a)"; else echo "elapsed: $HOURS h"; fi

# 指定番号のスナップショットだけを取り出す（ヘッダから === end === まで）
extract() { awk -v T="$2" '/^=== soak snapshot /{k++; inS = (k == T)} inS {print} /^=== end ===/{inS = 0}' "$1"; }
extract "$LOG" 1 > "$T/first"; extract "$LOG" "$N" > "$T/last"

# プロセス行を「tag 役割#番号 pid rss fds」にする
procs() { awk -v tag="$2" '
  /^--- processes/ { s = 1; next }
  /^--- /          { s = 0 }
  s && $1 ~ /^[0-9]+$/ && NF >= 5 {
    role = ""
    if (index($0, "--core-listen")) role = "core"
    else if (index($0, "--core-dir") || index($0, "--shard-id")) role = "gateway"
    else if (index($0, "bin/botd")) role = "botd"
    if (role == "") next
    n[role]++; print tag, role "#" n[role], $1, $4, $5
  }' "$1"; }
{ procs "$T/first" 1; procs "$T/last" 2; } > "$T/procs"

echo "--- processes (role#n = n-th by pid; first -> last) ---"
out=$(awk -v H="$HOURS" -v V="$V" '
  $1 == 1 { pid1[$2] = $3; rss1[$2] = $4; fds1[$2] = $5; next }
  { pid2[$2] = $3; rss2[$2] = $4; fds2[$2] = $5 }
  END {
    for (k in pid2) {
      if (!(k in pid1)) { printf "%s: new in last snapshot (pid %s)\n", k, pid2[k]; printf "restarts\tWATCH\t%s appeared\n", k >> V; continue }
      d = rss2[k] - rss1[k]; fd = fds2[k] - fds1[k]
      pct = (rss1[k] > 0) ? d * 100 / rss1[k] : 0
      ph = (H > 0) ? sprintf("%+.0f", d / H) : "n/a"
      flag = (pid1[k] != pid2[k]) ? "  [pid changed: restarted]" : ""
      printf "%s pid %s->%s  rss %s->%s kB (%+d, %s kB/h)  fds %s->%s (%+d)%s\n", k, pid1[k], pid2[k], rss1[k], rss2[k], d, ph, fds1[k], fds2[k], fd, flag
      st = ((rss1[k] > 0 && pct > 20) || (H > 0 && d / H > 5000)) ? "WATCH" : "OK"
      printf "RSS\t%s\t%s rss %s->%s (%+.0f%%, %s kB/h)\n", st, k, rss1[k], rss2[k], pct, ph >> V
      fst = (fd > 0) ? "WATCH" : "OK"
      printf "fds\t%s\t%s %s->%s\n", fst, k, fds1[k], fds2[k] >> V
      if (pid1[k] != pid2[k]) printf "restarts\tWATCH\t%s pid %s->%s\n", k, pid1[k], pid2[k] >> V
      else printf "restarts\tOK\t%s\n", k >> V
    }
    for (k in pid1) if (!(k in pid2)) { printf "%s: gone in last snapshot\n", k; printf "restarts\tWATCH\t%s gone\n", k >> V }
  }' "$T/procs" | sort)
if [ -n "$out" ]; then echo "$out"; else echo "(no botd / core / gateway process lines matched)"; fi

echo "--- files (bytes): first -> last ---"
fl() { awk -v tag="$2" '
  /^--- files \(bytes\)/ { s = 1; next }
  /^--- / || /^sockets\// { s = 0 }
  s && $1 ~ /^[0-9]+$/ { p = $0; sub(/^[0-9]+ +/, "", p); print tag "\t" p "\t" $1 }' "$1"; }
{ fl "$T/first" 1; fl "$T/last" 2; } | awk -F'\t' -v V="$V" '
  $1 == 1 { sz1[$2] = $3; next }
  { sz2[$2] = $3 }
  END {
    for (p in sz2) {
      st = "OK"
      if (p in sz1) {
        d = sz2[p] - sz1[p]
        # 小さいログが数行増えただけ（20% 超でも 100 KiB 未満）は警告しない。ローテーションは 8 MiB。
        if (sz1[p] > 0 && d * 100 / sz1[p] > 20 && d > 102400) st = "WATCH"
        printf "  %s  %s -> %s (%+d)\n", p, sz1[p], sz2[p], d
      } else printf "  %s  new in last snapshot (%s)\n", p, sz2[p]
      printf "files\t%s\t%s\n", st, p >> V
    }
    for (p in sz1) if (!(p in sz2)) printf "  %s  gone in last snapshot\n", p
  }' | sort

# 数値の取り出し（gateway.log の各行は同じ key を持つので合計する）
nr() { awk '/^unit:/ { for (i = 1; i <= NF; i++) if ($i ~ /^NRestarts=/) { sub(/^NRestarts=/, "", $i); print $i; exit } }' "$1"; }
gw() { awk -v K="$2" '
  /^--- gateway.log signals/ { s = 1; next }
  /^--- /                    { s = 0 }
  s { for (i = 1; i <= NF; i++) if (index($i, K "=") == 1) { v = $i; sub(/^[^=]*=/, "", v); t += v; hit = 1 } }
  END { if (hit) print t; else print "n/a" }' "$1"; }
jw() { awk '/^--- journal since boot/ { getline x; if (x ~ /^[0-9]+$/) print x; else print "n/a"; exit }' "$1"; }

echo "--- counters: first -> last ---"
a=$(nr "$T/first"); b=$(nr "$T/last")
echo "NRestarts: ${a:-n/a} -> ${b:-n/a}"
rx=$(gw "$T/first" resumed); ry=$(gw "$T/last" resumed)
gx=$(gw "$T/first" identify_grant_unavailable); gy=$(gw "$T/last" identify_grant_unavailable)
for K in generations closes worker_unavailable identify_grant_unavailable; do
  x=$(gw "$T/first" "$K"); y=$(gw "$T/last" "$K")
  if [ "$K" = worker_unavailable ] && isnum "$x" && isnum "$y"; then
    if { isnum "$gx" && ! isnum "$gy"; } || { ! isnum "$gx" && isnum "$gy"; }; then
      echo "$K (sum over gateway.log): n/a (counter definition changed)"
      add "$K" NA "counter definition changed"
      continue
    fi
    if ! isnum "$gx"; then echo 'worker_unavailable uses the historical all-unavailable-lines counter'; fi
  fi
  echo "$K (sum over gateway.log): $x -> $y"
  if isnum "$x" && isnum "$y"; then
    if [ "$K" = closes ] && [ "$y" -lt "$x" ]; then
      add "$K" WATCH "$x->$y (close log count decreased; check rotation)"
    elif [ "$K" = closes ] && [ "$y" -gt "$x" ]; then
      # Close events and successful Resumes are independent log signals.
      add "$K" WATCH "$x->$y (close log lines increased; inspect Gateway logs for outcomes)"
    elif [ "$y" -gt "$x" ]; then add "$K" WATCH "$x->$y"; else add "$K" OK "$x->$y"; fi
  else add "$K" NA "n/a"; fi
done
echo "resumed (sum over gateway.log): ${rx:-n/a} -> ${ry:-n/a}"
echo "Close and Resume counts are independent log signals."
ja=$(jw "$T/first"); jb=$(jw "$T/last"); echo "journal warnings since boot: $ja -> $jb"
if isnum "$a" && isnum "$b"; then
  if [ "$b" -gt "$a" ]; then add restarts WATCH "NRestarts $a->$b"; else add restarts OK "NRestarts $a->$b"; fi
else add restarts NA "NRestarts unknown"; fi
if isnum "$jb"; then
  if [ "$jb" -gt 0 ]; then add "journal warnings" WATCH "$jb at last snapshot"; else add "journal warnings" OK "0"; fi
else add "journal warnings" NA "n/a"; fi

echo "--- verdict ---"
for m in RSS fds restarts files generations closes worker_unavailable identify_grant_unavailable "journal warnings"; do
  awk -F'\t' -v m="$m" '
    $1 == m { n++; if ($2 == "WATCH") w = w (w == "" ? "" : "; ") $3; else if ($2 == "OK") ok++ }
    END { if (w != "") print m ": WATCH (" w ")"; else if (ok > 0) print m ": OK"; else print m ": n/a (no data)" }' "$V"
done
