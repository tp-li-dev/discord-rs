# Create Message の nonce 実機試験

Ubuntu の作業ツリーで実行する。試験専用チャンネルと、そのチャンネルに投稿できる試験用 Bot を用意する。実行前に、同一本文の 3 回の POST（同じ nonce で 2 回、異なる nonce の対照で 1 回）と、オプションを付けた場合の追加 1 回および試験メッセージの削除について許可を得る。本文は `discord-rs nonce probe <24桁のランダムな16進文字列>`。メンション通知は抑止する。必要な権限はチャンネルの閲覧と投稿。削除は試験 Bot が作成した自分のメッセージに対して行う。

## 実行

リポジトリのルートでビルドする。ここまでは Discord へ接続しない。

```bash
cargo build --manifest-path src/Cargo.toml --target-dir src/target --example nonce_probe
src/target/debug/examples/nonce_probe --help
```

同じ Bash で、承認済みのチャンネル ID と試験用 Bot token を入力する。token 入力は非表示で、実行するプログラムの環境変数にだけ渡す。

```bash
(
  set +x
  read -rp '試験チャンネル ID: ' nonce_probe_channel
  read -rsp '試験用 Bot token: ' NONCE_PROBE_BOT_TOKEN
  printf '\n'
  export NONCE_PROBE_BOT_TOKEN
  src/target/debug/examples/nonce_probe \
    --token-env NONCE_PROBE_BOT_TOKEN --channel-id "$nonce_probe_channel" \
    --yes-post --delay-seconds 30 --cleanup
)
```

`--delay-seconds 30` は 2 回目の成功応答の後で 30 秒待ち、元の nonce で 3 回目を送る。待ち時間中にチャンネルで最初の 2 回分の投稿数を確認できる。比較だけなら `--delay-seconds 30` を省く。後から目視で照合する場合は `--cleanup` を省き、確認後に許可の範囲で試験投稿を手動削除する。再実行ごとに新しい nonce を使う。

## 出力と報告

同じ ID が返り、対照で新しい ID が返る例（ID は例示、遅延試行の出力を省略）。

```text
post1 status=200 message_id=101
post2_start_elapsed_ms=200
post2 status=200 message_id=101
second_same_message_id=true
control status=200 message_id=102
control_distinct_message_id=true
result=deduplicated
cleanup message_id=101 status=204
cleanup message_id=102 status=204
```

同じ nonce でも別 ID が返る例。

```text
post1 status=200 message_id=101
post2_start_elapsed_ms=200
post2 status=200 message_id=102
second_same_message_id=false
control status=200 message_id=103
control_distinct_message_id=true
result=not_deduplicated
```

遅延試行では `post3_start_elapsed_ms`、`post3`、`third_same_as_first`、`third_same_as_second` が追加される。この 1 回の観測から正確な保持期間は決められない。失敗や対照が成立しない場合は `result=inconclusive` になる。429 や、HTTP ステータスが得られる前の期限切れでは、後続の投稿を止め、レート制限を迂回しないよう削除も見送る。応答を失った POST は ID が分からないことがあるため、チャンネルで実行時に表示した本文を照合し、残っている試験投稿を許可の範囲で削除する。

報告には `post1`、経過時間、`post2`、遅延試行を使った場合の `post3`、`control`、ID の比較、`result`、削除結果の行と、目視した投稿数を貼る。チャンネル ID やメッセージ ID を伏せる場合は、同一の ID を同一の別名に置き換える。Bot token は共有しない。条件を満たした結果を確認するまで運用の再試行フラグは無効のままにする。
