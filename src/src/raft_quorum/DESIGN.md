# leader 転送・crash takeover・mTLS RPC

仕様の基準は `docs/architecture.md` §5.3。選挙、投票、複製、commit、競合ログの処理は引き続き OpenRaft 0.9.26 が担う。新しい合意方式は実装しない。ここでは担当範囲内の変更と、既存 mTLS への接続に必要な境界を記録する。`docs/progress.md` は変更しない。

## leader への転送

`RaftNode::submit` はまずローカル OpenRaft に要求する。OpenRaft が `ForwardToLeader` を返したときだけ、固定 membership 内の宛先へ `RpcRequest::Forward` を送る。各ノードへの到達は最大1回、処理全体に1つの `write_timeout` を適用する。サーバー側では転送要求をさらに転送せず、ローカル OpenRaft の結果を返す。

`ForwardToLeader` は未受理の要求、または競合によってログから除かれ commit できなくなった要求に対する応答である。接続断、timeout、その他の Raft エラーを同じ再送条件にしない。これらは `OutcomeUnknown` とし、自動再送もローカル権限付与もしない。

認証済み RPC アダプターは証明書由来の送信元、要求の source、command の owner を一致させ、非ゼロ session、cluster scope、membership、宛先、nonce、応答種別を検査する。leader hint から未知ノードへ接続しない。`MemoryTransport` は同一プロセスの試験用であり、認証を提供しない。

## 異常終了した所有者からの引継ぎ

`QuorumCoordinator` の新APIを次の順に呼ぶ。

1. 障害検出・配置判断を行う信頼済み呼出元が `begin_takeover(key, expected_epoch)` を実行する。
2. `Command::BeginTakeover` を Raft でcommitし、expected epochが一致する保持中の予約を、新owner/sessionと `epoch + 1` へ置換する。この時点では新しいleaseを与えない。旧epochの `Renew` / `Release` はCASで拒否する。
3. 成功応答の受信後から、ローカル時計で `TAKEOVER_QUARANTINE = 300秒` を待つ。これは各ノードの `max_lease_ttl` に依存しない、既存実装の全体上限である。`takeover_ready_at` はローカルの予定時刻であり、権限ではない。
4. `finish_takeover(key, epoch, ttl)` で再度 `Renew` をcommitする。新leaseの期限はこの要求の開始時刻から計算し、遅い応答で延長しない。その成功後に限りローカルgateで新規受付を許可する。

旧所有者が直前に更新していた場合でも、その更新は新epochの確定より前に順序付けられる。新epoch確定後の更新は拒否される。したがって、時計が同じ速度で進み、停止中も必要な時間を計測し、すべての実行gateが元のleaseを守るという条件下で、成功応答後から全体最大TTLを待つことで旧leaseの有効区間を避ける。単なるheartbeat喪失や古いノードのローカル時計値を解除根拠にしない。

待機中の `release_quorum` は拒否する。ここで予約を解放すると、第三者の通常のAcquireが旧lease失効前に通ってしまうためである。同期 `release` は自分のローカルgateだけを閉じ、待機状態を消さない。

beginのtimeout・取消時はcommitが後から成立し得るため、待機開始時刻を作らない。同じsessionでは完了も解放もできない。新coordinator/sessionで現在epochに対する新しいBeginTakeoverをcommitし、全待機をやり直す。再起動後も永続予約からleaseや残り待機時間を復元しない。finishのtimeout・取消は権限を与えず待機状態を保持し、再試行は新しい要求開始時刻を使う。

`RaftNode::submit`、raw `Raft`、`Command` は信頼済み制御層の低水準APIである。状態機械は他ホストの期限経過やプロセス停止を証明できない。直接Releaseを投入する呼出元には、旧gate停止とtakeover待機完了の双方が必要である。通常のlease利用は `QuorumCoordinator` を通す。

## mTLS Raft RPC API

`TlsTransport::bind_raft_rpc` と `tls_transport::raft_rpc::{TlsRpcTransport, TlsRpcChannel}` を追加した。`tls_transport.rs` の変更は公開子moduleの宣言4行だけで、実装本体は `raft_quorum/tls_channel.rs` に置く。既存のcontrol transportの通信・queue ACK・証明書運用APIは変更しない。

接続の順序（実行可能な使用例は `src/tests/raft_mtls.rs` の `Cluster::start`）:

1. 既存の `TlsTransportConfig` でノード証明書・CA・membership・初期失効一覧・上限を設定し、`TlsTransport::bind` する。
2. `tls.bind_raft_rpc(loopback_address).await` で専用listenerを作る。`127.0.0.1:0` はOS割当ポートを使い、`rpc.local_addr()` で取得できる。現段階では親transportの設定にかかわらずloopback以外のbindを拒否する。
3. `rpc.channel()` を `AuthenticatedRpcAdapter::new(channel, scope, limits)` に渡し、そのadapterを `RaftNode::start` のtransportにする。各ノードに同じ非ゼロcluster IDと固定membershipを与える。明示的な `allow_test_mode` が必要。
4. `RpcEndpoint::new(node_id, scope, node.raft.clone(), limits)` を `rpc.set_endpoint` に登録する。ノードID不一致と二重登録は拒否する。`rpc.set_peer(NodeId(peer), rpc_address)` で相手のRPC専用アドレスを設定する。control用のpeerアドレスは自動流用しない。
5. 新規クラスタだけ1ノードで `initialize` する。再起動は `StorageMode::Open` と同じscopeを使い、再initializeしない。
6. 終了時は `rpc.shutdown().await`、`node.shutdown().await`、`tls.shutdown().await` の順に停止する。RPC listenerは独立した寿命を持ち、親control listenerの停止では閉じない。RPC handleの停止・dropはchannelの全cloneを無効化する。

専用ALPN `botd-raft/1`、`BRPC` greetingとversion、u32長付き要求・応答で既存control protocolから分離する。相互TLS認証を終えた証明書のノードIDを `RpcEndpoint::handle` に渡す。adapter/endpointがsource、command owner、destination、scope、nonce、応答種別を検査し、OpenRaftの処理結果を返す。control queueへの投入ACKをcommitとして扱わない。

親transportの証明書・検証器・失効/更新policyを `Arc` で共有するため、既存の `set_revocations`、`rotate_peer_certificate`、`rotate_identity` は生成済みRPC handleにも反映される。handshake後、要求受付、dispatchのpoll、応答書込みのpoll、応答受領後にpolicyを再検査する。各pollとpolicy更新を同期し、標準lockをawait中に保持しない。既にOpenRaftへ渡った要求は、取消・timeout・失効後にもcommitし得る。失敗は取消確認でもlease付与でもなく、自動再送しない。

接続数、queue長、adapter/endpointのin-flight数、frame長、handshake/read/write/dispatchの期限を制限する。長さprefixとbodyには1つのread期限を使い、上限検査前にbodyを確保しない。channel単体にもqueue待ちを含む総期限を設ける。RPCの上限は親transportからコピーするが、接続枠・queue・統計はcontrol listenerと別である。snapshotも上限内の単一frameを使い、chunk分割は未実装。

失効・更新policyは既存API同様にローカルメモリ上だけであり、クラスタへの配布や再起動後の自動復元は行わない。初期失効一覧を設定し、必要な更新policyを復元してからendpointと経路を有効化する。RPC統計の成功は応答受領であり、epoch変更のcommit成功数ではない。

## 適用範囲

本番は既定無効。`enable_production = true` でも `ProductionNotReady` を返す既存guardを維持する。試験利用には `allow_test_mode = true` が必要。botd/Gateway/Coreへの接続、プロセスの停止watchdog、RESTの外部副作用のfencingは今回実装しない。

手動時計での300秒経過は実時間やOS suspendの試験ではない。異なる時計速度、VM pause、Linuxでの実行、複数ホスト、電源断、実Discord、性能、異版ノード混在は別途検証が必要。最大TTLの短縮にはクラスタ全体の永続policyと移行手順が必要であり、ノード設定だけでは安全に短縮できない。

## 検証記録（2026-11-12）

Windows / Rust 1.90.0。`src/` で `CARGO_NET_OFFLINE=true`、`CARGO_PROFILE_DEV_DEBUG=0`、`CARGO_INCREMENTAL=0` を設定して実行した。新規依存・lockfile変更・commitなし。

| 実行 | 結果 |
|---|---|
| `cargo test --offline --lib raft_quorum::` | 15件成功 |
| `cargo test --offline --test raft_quorum --test raft_quorum_storage --test raft_forwarding --test raft_quorum_handoff` | 29件成功、補助subprocess試験1件は単独実行ignore（親の強制終了・復旧試験は成功） |
| `cargo clippy --offline --lib --test raft_quorum --test raft_quorum_storage --test raft_forwarding --test raft_quorum_handoff -- -D warnings` | 成功 |
| 変更したRust 4ファイルの `rustfmt --check --edition 2024 --config skip_children=true` | 成功 |
| `git diff --check` | 成功 |

転送の新規試験は送信元偽装・scope/nonce/応答種別・leader hintと、実OpenRaftでのfollower拒否/leader転送/epoch CAS/停止時の結果不明を確認した。引継ぎの新規5試験は最大TTL境界、早期Release禁止、再起動後の再隔離、取消後の遅延commit、finish遅延、quorum不足、競合Fenceを確認した。

上表は前回のleader転送・takeover作業の記録で、配送はMemoryTransportまたは明示的なchannel fixtureだった。以下の継続作業でmTLS実TCPの試験を追加する。Linux、workspace全体、本番Gateway/Coreの実行経路の検証はこの記録に含めない。

## mTLS 継続作業の検証記録（2026-11-12）

Windows / Rust 1.90.0、`src/` で `CARGO_NET_OFFLINE=true`、`CARGO_PROFILE_DEV_DEBUG=0`、`CARGO_INCREMENTAL=0`。cargoはすべて `--offline` で実行した。新規依存とlockfileの変更はない。

| 実行 | 最終結果 |
|---|---|
| `cargo test --offline --lib` | 142件成功（新しい失効poll境界2件を含む） |
| `cargo test --offline --test raft_mtls --test raft_tls_channel` | 12件成功（3ノード7件、TLS境界5件） |
| `cargo test --offline --test raft_quorum --test raft_quorum_storage --test raft_forwarding --test raft_quorum_handoff --test tls_transport --test cert_rotation` | 77件成功、補助subprocess試験1件ignore（親の強制終了・復旧試験は成功） |
| `cargo clippy --offline --lib --test raft_mtls --test raft_tls_channel --test raft_quorum --test raft_quorum_storage --test raft_forwarding --test raft_quorum_handoff --test tls_transport --test cert_rotation -- -D warnings` | 成功 |
| 変更および前回未コミットのRust 8ファイルに `rustfmt --check --edition 2024 --config skip_children=true` | 成功 |
| `git diff --check` | 成功 |

合計231件成功、新規14件。3ノード試験の選挙・投票・複製・commitは実OpenRaft、配送は独立したloopback TCP/mTLS listener、永続化は実ファイルを使用した。メモリ内配送への置換はない。

- リーダー選出、2 followerからの競合epoch CAS、leader転送、Bot/Shard別の独立性を確認。
- 現leaderを停止し、残り2ノードの再選出と書込み継続を確認。さらに1ノード停止後は書込み・lease更新を拒否し、状態機械のepochを進めない。lease期限は手動時計で検査しておりOS suspendの証明ではない。
- 全3ノードをshutdown後に同じファイルからreopenし、epochと保持中予約が残ること、新coordinatorに旧leaseが復元されないこと、次のCASが単調増加することを確認。
- 停止したfollowerの再参加で、leaderのログpurge後に実snapshot RPCで追いつき、epoch CASを継続できることを確認。
- 失効ノードのVote、Append、Snapshot、Forwardを拒否し、高termの要求でも受信ノードのtermを進めない。正常2ノードは書込みを継続する。
- 作成済みRPC handleが親TLSの証明書更新を使用し、旧serialの猶予終了後はRPCを拒否する。既存TLS 25件・失効/更新23件も成功。
- 専用ALPN、平文・証明書なしの拒否、scope/source/target偽装拒否、接続・frame上限、部分prefixの期限、取消時の接続枠解放、shutdown後のclone拒否を確認。
- handshake後の失効、remote処理後に応答だけ待機している間の失効を実TCPで検査。dispatchの再pollとブロック中の書込みpollは制御したFuture/writerの単体試験で検査した。後者は実socketのbackpressure試験ではない。
- mTLS adapterでも未指定test modeと本番指定を拒否し、拒否前にstorageを作らない。

新規TLS境界試験の初回は平文TCPに返るTLS alertをアプリケーション応答と誤認して1件失敗した。平文の場合は有限長のTLS alert/切断を受け入れ、TLS拒否診断と受信件数0で判定するよう試験を修正し、再実行で5件成功した。製品側の認証条件は緩和していない。

性能ベンチマークは未実施。今回の再起動試験は同一Windowsプロセス内のノードshutdown/reopenで、別プロセス・別ホスト・電源断のmTLSクラスタ試験ではない。Linux、workspace全体、実Discord、botd/Gateway/Coreへの本番配線、runtime fencing、OS suspend、CA更新、失効policyの配布・永続化、巨大snapshotのchunk転送は未検証または未実装。本番起動拒否を維持する。

今回の変更ファイルは `raft_quorum.rs`（説明更新）、`raft_quorum/transport.rs`（接続説明とendpoint ID getter）、`raft_quorum/tls_channel.rs`（RPC実装・単体試験）、`tls_transport.rs`（公開module宣言）、`tests/raft_mtls.rs`、`tests/raft_tls_channel.rs`、この `DESIGN.md`。前回の転送・takeover差分と新規試験は保持した。`docs/progress.md`・`docs/handoff.md` は変更せず、commitも行っていない。
