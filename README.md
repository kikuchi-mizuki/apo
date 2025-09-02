# Googleカレンダー→AI抽出→スプレッドシート連携システム

Googleカレンダーの予定から **タイトル先頭に「【B】」が付いたイベントのみ** を抽出し、AIで「会社名」「人の名前」「日時」を高精度に抽出・正規化して、Googleスプレッドシートに蓄積・管理できる仕組みを提供します。

## 🎯 主な機能

- **【B】イベント自動抽出**: タイトル先頭の「【B】」パターンでイベントを自動判定
- **AI抽出**: LLM + 規則ベースのハイブリッド抽出で会社名・人名を高精度抽出
- **データ正規化**: 会社名の表記ゆれ吸収、人名の正規化
- **スプレッドシート連携**: Googleスプレッドシートへの自動upsert
- **監査ログ**: 詳細な実行ログとエラーハンドリング
- **CLI操作**: コマンドラインからの簡単操作

## 📋 要件

- Python 3.11+
- Google Calendar API アクセス権限
- Google Sheets API アクセス権限
- OpenAI API キー（AI抽出用）

## 🚀 セットアップ

### 1. リポジトリのクローン

```bash
git clone <repository-url>
cd apo
```

### 2. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 3. 環境変数の設定

`env.sample` を `.env` にコピーして設定してください：

```bash
cp env.sample .env
```

`.env` ファイルを編集して以下の値を設定：

```env
# Google API設定
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
GOOGLE_CALENDAR_ID=primary
GOOGLE_SPREADSHEET_ID=your-spreadsheet-id-here

# OpenAI設定
OPENAI_API_KEY=your-openai-api-key-here

# ログ設定
LOG_LEVEL=INFO
LOG_FILE=logs/sync.log

# 同期設定
SYNC_WINDOW_PAST_DAYS=30
SYNC_WINDOW_FUTURE_DAYS=60
SYNC_INTERVAL_MINUTES=15
```

### 4. Google API認証の設定

#### サービスアカウントキーの作成

1. [Google Cloud Console](https://console.cloud.google.com/) にアクセス
2. プロジェクトを作成または選択
3. Google Calendar API と Google Sheets API を有効化
4. サービスアカウントを作成
5. JSONキーファイルをダウンロード
6. ダウンロードしたファイルのパスを `GOOGLE_APPLICATION_CREDENTIALS` に設定

#### カレンダーとスプレッドシートの共有設定

- **Googleカレンダー**: サービスアカウントのメールアドレスに読み取り権限を付与
- **Googleスプレッドシート**: サービスアカウントのメールアドレスに編集権限を付与

### 5. 設定ファイルの確認

`config.yaml` の設定を必要に応じて調整してください。

## 🎮 使用方法

### 基本的な同期実行

```bash
# 過去30日〜未来60日の同期
python main.py sync

# カスタム期間での同期
python main.py sync --past 7 --future 30

# 特定の日付範囲での同期
python main.py sync --start-date 2025-01-01 --end-date 2025-01-31

# ドライラン（実際の更新は行わない）
python main.py sync --dry-run
```

### システム状況の確認

```bash
# 全体的な状況確認
python main.py status

# 設定情報の表示
python main.py config
```

### データ管理

```bash
# 古いレコードのクリーンアップ
python main.py cleanup --days 90

# データのエクスポート
python main.py export --format csv --output data.csv
python main.py export --format json --output data.json
```

### システムテスト

```bash
# 全機能のテスト実行
python main.py test
```

## 📊 出力データ形式

スプレッドシートの "Bookings" シートに以下の列でデータが保存されます：

| 列名 | 説明 |
|------|------|
| event_id | カレンダーの一意ID |
| title | 元タイトル |
| company_name | 抽出された会社名 |
| person_names | 抽出された人名（JSON配列） |
| start_datetime | 開始日時（Asia/Tokyo） |
| end_datetime | 終了日時（Asia/Tokyo） |
| timezone | タイムゾーン |
| attendees | 出席者情報（JSON配列） |
| location | 場所・URL |
| source_calendar | 取得元カレンダーID |
| extracted_confidence | 抽出信頼度（0.0-1.0） |
| status | ステータス（active/removed/cancelled） |
| updated_at | 同期時刻 |
| run_id | 同期実行ID |

## 🔧 設定オプション

### カレンダー設定

```yaml
calendar:
  calendar_id: "primary"  # 対象カレンダーID
  sync_window_past_days: 30    # 過去同期日数
  sync_window_future_days: 60  # 未来同期日数
  max_results: 1000            # 最大取得件数
```

### 【B】イベント判定設定

```yaml
event_filter:
  b_event_pattern: "^[　\\s]*【B】"  # 正規表現パターン
  allow_bracket_variations: false    # 括弧バリエーション許可
```

### AI抽出設定

```yaml
ai_extraction:
  provider: "openai"           # LLMプロバイダー
  model: "gpt-4o-mini"        # モデル名
  confidence_threshold: 0.8    # 信頼度閾値
  max_retries: 3               # 最大リトライ回数
```

## 📝 【B】イベントの例

以下のようなタイトルのイベントが自動抽出されます：

- `【B】ABC株式会社 / 田中様 / オンライン面談`
- `【B】(仮) サンプル商事×山田さん`
- `【B】株式会社テスト 営業アポ`
- `　【B】ミーティング` （先頭のスペースも許容）

## 🔍 抽出ロジック

### 1. 規則ベース抽出（1次）

- 会社名の接尾語パターン（株式会社、Inc.等）
- メールドメイン→企業名辞書
- 既存台帳との照合

### 2. AI抽出（2次）

- LLMによる自然言語理解
- タイトル・説明・出席者からの抽出
- 信頼度スコア付与

### 3. 結果統合・バリデーション（3次）

- 規則ベースとAI結果の統合
- データの正規化・クリーニング
- 最終的な信頼度計算

## 📈 パフォーマンス

- **処理速度**: 1,000件/分を目標
- **抽出精度**: 
  - 日時: 100%（Googleカレンダー値）
  - 会社名: 80-95%
  - 人名: 80-95%

## 🚨 トラブルシューティング

### よくある問題

1. **認証エラー**
   - サービスアカウントキーのパスを確認
   - カレンダー・スプレッドシートの共有設定を確認

2. **API制限エラー**
   - 設定の `max_results` を調整
   - 実行間隔を長く設定

3. **抽出精度が低い**
   - `confidence_threshold` を調整
   - 既存の会社名辞書を充実

### ログの確認

```bash
# ログファイルの確認
tail -f logs/sync.log

# エラーログの確認
grep "ERROR" logs/sync.log
```

## 🔄 定期実行

### cronでの定期実行

```bash
# 15分毎に実行
*/15 * * * * cd /path/to/apo && python main.py sync

# 1時間毎に実行
0 * * * * cd /path/to/apo && python main.py sync
```

### systemdでの定期実行

```ini
[Unit]
Description=Calendar Sync Service
After=network.target

[Service]
Type=oneshot
User=your-user
WorkingDirectory=/path/to/apo
ExecStart=/usr/bin/python3 main.py sync
Environment=PATH=/usr/bin:/usr/local/bin

[Install]
WantedBy=multi-user.target
```

## 🤝 貢献

1. このリポジトリをフォーク
2. フィーチャーブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 📞 サポート

問題や質問がある場合は、GitHubのIssuesページで報告してください。

---

**注意**: このシステムは機密情報を扱います。適切なセキュリティ設定とアクセス制御を行ってください。
