#!/usr/bin/env bash
# Bot の token を、永続の環境変数ファイルに安全に保存する（画面にも履歴にも出さない）。
#
# 使い方（root で実行。token は対話で入力する。引数にもコマンドにも書かない）:
#   sudo ./set-token.sh [ENV_FILE] [VAR_NAME]
#     ENV_FILE  既定 /etc/discord-rs/botd.env
#     VAR_NAME  既定 DISCORD_TEST_BOT_TOKEN（bots.toml の token_env と同じ名前にする）
#
# 保存先は root:discord-rs の 0640。systemd の LoadCredential= で botd に渡る。
# /etc/environment には書かない（誰でも読め、全プロセスに入るため）。
# 既存のファイルの他の行は保持し、同名の変数だけを置き換える。
set -eu
ENV_FILE="${1:-/etc/discord-rs/botd.env}"
VAR="${2:-DISCORD_TEST_BOT_TOKEN}"
BOTD=/opt/discord-rs/bin/botd

[ "$(id -u)" = 0 ] || { echo "root で実行してください: sudo $0"; exit 2; }
"$BOTD" --validate-token-name "$VAR"
if [ -e "$ENV_FILE" ] || [ -L "$ENV_FILE" ]; then
  "$BOTD" --check-env-file "$ENV_FILE"
fi

getent group discord-rs >/dev/null || groupadd --system discord-rs
dir=$(dirname "$ENV_FILE")
install -d -m 0750 -o root -g discord-rs "$dir"

printf 'token を貼り付けて Enter（画面には何も表示されません）: ' >&2
IFS= read -rs token
printf '\n' >&2
# 前後の空白・改行・引用符の混入を除く（貼り付け時の事故対策）
token=${token//$'\r'/}
token=${token#"${token%%[![:space:]]*}"}
token=${token%"${token##*[![:space:]]}"}
token=${token#\"}; token=${token%\"}
case "$token" in
  '') echo "空です。保存しません。" >&2; exit 1 ;;
  *[[:space:]]*) echo "空白を含んでいます。貼り付けを確認してください。保存しません。" >&2; exit 1 ;;
esac

umask 077
tmp=$(mktemp "$dir/.botd.env.XXXXXX") || exit 1
trap 'rm -f -- "$tmp"' EXIT
if [ -f "$ENV_FILE" ]; then
  # 同名の行だけを除いて残りを保持（token の値は awk の引数に出さない）
  awk -F= -v name="$VAR" '$1 != name' "$ENV_FILE" > "$tmp"
fi
printf '%s=%s\n' "$VAR" "$token" >> "$tmp"
"$BOTD" --check-env-file "$tmp"
chown root:discord-rs "$tmp"
chmod 0640 "$tmp"
mv -f "$tmp" "$ENV_FILE"
unset token

echo "保存しました: $ENV_FILE ($(stat -c '%a %U:%G' "$ENV_FILE"))、変数: $VAR（値の長さ: $(grep "^${VAR}=" "$ENV_FILE" | cut -d= -f2- | tr -d '\n' | wc -c) 文字）"
echo "systemd では LoadCredential で botd に渡ります。端末で使う場合: set -a; . $ENV_FILE; set +a （root または discord-rs グループで）"
