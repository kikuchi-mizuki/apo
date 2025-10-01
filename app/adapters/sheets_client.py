"""
GoogleスプレッドシートAPIクライアント
"""
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from loguru import logger

from ..core.schemas import BookingRecord
from ..core.config import config_manager


class GoogleSheetsClient:
    """GoogleスプレッドシートAPIクライアント"""
    
    def __init__(self, credentials_path: Optional[str] = None):
        self.credentials_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        self.spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
        self.client = None
        self.spreadsheet = None
        self.worksheet = None
        
        # 設定からシート名を取得
        sheets_config = config_manager.get_spreadsheet_config()
        self.sheet_name = sheets_config.get('sheet_name', 'Bookings')
        
        # クライアントを初期化
        self._initialize_client()
    
    def _initialize_client(self):
        """Googleスプレッドシートクライアントを初期化"""
        try:
            if not self.spreadsheet_id:
                raise ValueError("GOOGLE_SPREADSHEET_IDが設定されていません")
            
            if self.credentials_path and os.path.exists(self.credentials_path):
                # サービスアカウントキーを使用
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=[
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                )
                logger.info(f"サービスアカウントキーを使用: {self.credentials_path}")
            else:
                # 環境変数から認証情報を取得
                credentials = service_account.Credentials.from_service_account_info(
                    info=json.loads(os.getenv('GOOGLE_SERVICE_ACCOUNT_INFO', '{}')),
                    scopes=[
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                )
                logger.info("環境変数から認証情報を取得")
            
            # 認証情報を更新
            if credentials.expired:
                credentials.refresh(Request())
            
            # クライアントを構築
            self.client = gspread.authorize(credentials)
            logger.info("Googleスプレッドシートクライアントを初期化しました")
            
            # スプレッドシートとワークシートを取得
            self._open_spreadsheet()
            
        except Exception as e:
            logger.error(f"Googleスプレッドシートクライアントの初期化に失敗しました: {e}")
            raise
    
    def _open_spreadsheet(self):
        """スプレッドシートとワークシートを開く"""
        try:
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            logger.info(f"スプレッドシートを開きました: {self.spreadsheet.title}")
            
            # ワークシートを取得または作成
            try:
                self.worksheet = self.spreadsheet.worksheet(self.sheet_name)
                logger.info(f"ワークシートを開きました: {self.sheet_name}")
            except WorksheetNotFound:
                # ワークシートが存在しない場合は作成
                self.worksheet = self.spreadsheet.add_worksheet(
                    title=self.sheet_name,
                    rows=1000,
                    cols=20
                )
                logger.info(f"ワークシートを作成しました: {self.sheet_name}")
                
                # ヘッダー行を設定
                self._setup_headers()
            
        except Exception as e:
            logger.error(f"スプレッドシートのオープンに失敗しました: {e}")
            raise
    
    def _setup_headers(self):
        """ヘッダー行を設定"""
        try:
            headers = [
                'event_id', 'title', 'company_name', 'person_names',
                'start_datetime', 'end_datetime', 'timezone',
                'attendees', 'location', 'source_calendar',
                'extracted_confidence', 'status', 'updated_at', 'run_id'
            ]
            
            self.worksheet.update('A1:N1', [headers])
            logger.info("ヘッダー行を設定しました")
            
        except Exception as e:
            logger.error(f"ヘッダー行の設定に失敗しました: {e}")
            raise

    # ===== ここからシンプル出力（会社名・人名・日付のみ）サポート =====
    def _ensure_simple_sheet(self) -> gspread.Worksheet:
        """シンプル出力用ワークシート（Bookings_Simple）を取得/作成"""
        try:
            try:
                ws = self.spreadsheet.worksheet("Bookings_Simple")
                # 既存シートの列構成を確認し、必要ならマイグレーション
                self._migrate_simple_sheet_structure(ws)
                # A列(event_id)を非表示にする
                self._hide_simple_event_id(ws)
                return ws
            except WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title="Bookings_Simple", rows=1000, cols=4)
                # ヘッダー設定: event_id, 日付, 会社名, 名前
                ws.update('A1:D1', [["event_id", "date", "company_name", "person_names"]])
                logger.info("シンプル出力シートを作成しました: Bookings_Simple")
                # A列(event_id)を非表示にする
                self._hide_simple_event_id(ws)
                return ws
        except Exception as e:
            logger.error(f"シンプル出力シートの準備に失敗しました: {e}")
            raise

    def upsert_simple_record(self, record: 'BookingRecord') -> bool:
        """シンプル出力用にレコードをupsert（event_idベース、同じ日付の重複も防止）"""
        try:
            ws = self._ensure_simple_sheet()
            
            # 全データを取得（生の値で）
            all_values = ws.get_all_values()
            if len(all_values) <= 1:  # ヘッダーのみ
                all_values = []
            else:
                all_values = all_values[1:]  # ヘッダーを除く
            
            date_str = record.start_datetime.strftime('%Y-%m-%d')
            person_names_list = json.loads(record.person_names)
            person_names_str = ', '.join(person_names_list)
            
            # 1. event_idで既存レコードを検索
            existing_row = None
            for i, row in enumerate(all_values, start=2):  # ヘッダー行をスキップ
                if len(row) > 0 and row[0] == record.event_id:  # A列（event_id）で比較
                    existing_row = i
                    break
            
            # 2. event_idが見つからない場合、同じ日付+同じ人のレコードを検索
            if existing_row is None:
                for i, row in enumerate(all_values, start=2):
                    if (len(row) >= 4 and 
                        row[1] == date_str and  # B列（date）で比較
                        row[3] == person_names_str):  # D列（person_names）で比較
                        existing_row = i
                        logger.info(f"同じ日付+同じ人の既存レコードを発見: 行{existing_row}")
                        break

            new_row_data = [record.event_id, date_str, record.company_name or '', person_names_str]
            logger.info(f"書き込むデータ: {new_row_data}")

            if existing_row:
                # 既存レコードを更新
                ws.update(f'A{existing_row}:D{existing_row}', [new_row_data])
                logger.info(f"シンプル出力を更新しました: 行{existing_row}")
            else:
                # 新規レコードを追加（明示的に範囲を指定）
                # まず空行を追加してから、その行にデータを書き込み
                ws.append_row(['', '', '', ''])  # 空行を追加
                last_row = len(ws.get_all_values())  # 最後の行番号を取得
                ws.update(f'A{last_row}:D{last_row}', [new_row_data])
                logger.info(f"シンプル出力を追加しました: 行{last_row}")
            
            return True
        except Exception as e:
            logger.error(f"シンプル出力のupsertに失敗しました: {e}")
            return False

    # ====== シンプルシートからの行読み出し・書き戻し ======
    def read_simple_rows(self) -> List[Dict[str, str]]:
        """Bookings_Simpleの全行を辞書で取得（ヘッダー: event_id, date, company_name, person_names）"""
        ws = self._ensure_simple_sheet()
        records = ws.get_all_records()
        return records

    def write_simple_event_id(self, row_index: int, event_id: str) -> None:
        """Bookings_Simpleの指定行にevent_idを書き戻す（row_indexは2始まり）"""
        ws = self._ensure_simple_sheet()
        ws.update(f'A{row_index}:A{row_index}', [[event_id]])

    def _hide_simple_event_id(self, ws: gspread.Worksheet) -> None:
        """Bookings_SimpleのA列(event_id)を表示状態にする（非表示を解除）"""
        try:
            sheet_id = ws.id
            body = {
                'requests': [
                    {
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'COLUMNS',
                                'startIndex': 0,
                                'endIndex': 1
                            },
                            'properties': {
                                'hiddenByUser': False
                            },
                            'fields': 'hiddenByUser'
                        }
                    }
                ]
            }
            self.spreadsheet.batch_update(body)
            logger.info("Bookings_Simpleのevent_id列を表示しました")
        except Exception as e:
            logger.warning(f"event_id列の表示に失敗しました: {e}")

    def _migrate_simple_sheet_structure(self, ws: gspread.Worksheet) -> None:
        """既存のBookings_Simpleが3列(B:D)運用だった場合、A列を挿入しヘッダーを整える"""
        try:
            headers = ws.row_values(1)
            if not headers:
                return
            # 既にevent_idヘッダーがあるなら何もしない
            if len(headers) >= 1 and headers[0] == 'event_id':
                return
            # A列を1列挿入（先頭に空列を追加して既存B:DをC:Eへずらすのではなく、
            # Google Sheetsの仕様上 InsertDimension で先頭に1列追加すると既存B以降が右にシフト）
            sheet_id = ws.id
            body = {
                'requests': [
                    {
                        'insertDimension': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'COLUMNS',
                                'startIndex': 0,
                                'endIndex': 1
                            },
                            'inheritFromBefore': False
                        }
                    }
                ]
            }
            self.spreadsheet.batch_update(body)
            # ヘッダーを書き込み
            ws.update('A1:D1', [["event_id", "date", "company_name", "person_names"]])
            logger.info("Bookings_Simpleを4列構成にマイグレーションしました")
        except Exception as e:
            logger.warning(f"シンプルシートのマイグレーションに失敗しました: {e}")

    def append_simple_rows(self, rows: List[List[str]]) -> bool:
        """シンプル出力用に行を追記（date, company_name, person_names）"""
        if not rows:
            return True
        try:
            ws = self._ensure_simple_sheet()
            ws.append_rows(rows)
            logger.info(f"シンプル出力を追記しました: {len(rows)}行")
            return True
        except Exception as e:
            logger.error(f"シンプル出力の追記に失敗しました: {e}")
            return False
    
    def upsert_booking_records(self, records: List[BookingRecord]) -> Dict[str, int]:
        """予約記録をupsert（存在すれば更新、なければ追加）"""
        if not records:
            return {'upserted': 0, 'errors': 0}
        
        try:
            # 既存のレコードを取得
            existing_records = self._get_existing_records()
            
            upserted_count = 0
            error_count = 0
            
            for record in records:
                try:
                    if self._upsert_single_record(record, existing_records):
                        upserted_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"レコードのupsertに失敗しました: {record.event_id} - {e}")
                    error_count += 1
            
            logger.info(f"upsert完了: {upserted_count}件成功, {error_count}件失敗")
            return {'upserted': upserted_count, 'errors': error_count}
            
        except Exception as e:
            logger.error(f"upsert処理でエラーが発生しました: {e}")
            raise
    
    def _get_existing_records(self) -> Dict[str, int]:
        """既存のレコードのevent_idと行番号のマッピングを取得"""
        try:
            # 全データを取得
            all_values = self.worksheet.get_all_values()
            if len(all_values) <= 1:  # ヘッダーのみ
                return {}
            
            existing_records = {}
            for i, row in enumerate(all_values[1:], start=2):  # ヘッダーを除く
                if len(row) > 0 and row[0]:  # event_idが存在する場合
                    existing_records[row[0]] = i
            
            return existing_records
            
        except Exception as e:
            logger.error(f"既存レコードの取得に失敗しました: {e}")
            return {}
    
    def _upsert_single_record(self, record: BookingRecord, 
                             existing_records: Dict[str, int]) -> bool:
        """単一レコードをupsert"""
        try:
            # レコードをリストに変換
            record_values = self._record_to_values(record)
            
            if record.event_id in existing_records:
                # 既存レコードを更新
                row_number = existing_records[record.event_id]
                self.worksheet.update(f'A{row_number}:N{row_number}', [record_values])
                logger.debug(f"レコードを更新しました: {record.event_id} (行{row_number})")
            else:
                # 新規レコードを追加
                self.worksheet.append_row(record_values)
                logger.debug(f"レコードを追加しました: {record.event_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"レコードのupsertに失敗しました: {record.event_id} - {e}")
            return False
    
    def _record_to_values(self, record: BookingRecord) -> List[Any]:
        """BookingRecordをスプレッドシート用の値リストに変換"""
        return [
            record.event_id,
            record.title,
            record.company_name or '',
            record.person_names,
            record.start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            record.end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            record.timezone,
            record.attendees,
            record.location or '',
            record.source_calendar,
            record.extracted_confidence or '',
            record.status,
            record.updated_at.strftime('%Y-%m-%d %H:%M:%S'),
            record.run_id or ''
        ]
    
    def get_booking_records(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """予約記録を取得"""
        try:
            # 全データを取得
            all_values = self.worksheet.get_all_values()
            if len(all_values) <= 1:  # ヘッダーのみ
                return []
            
            # ヘッダーを除いてデータを処理
            headers = all_values[0]
            data_rows = all_values[1:]
            
            if limit:
                data_rows = data_rows[:limit]
            
            records = []
            for row in data_rows:
                if len(row) >= len(headers):
                    record = {}
                    for i, header in enumerate(headers):
                        record[header] = row[i] if i < len(row) else ''
                    records.append(record)
            
            logger.info(f"予約記録を取得しました: {len(records)}件")
            return records
            
        except Exception as e:
            logger.error(f"予約記録の取得に失敗しました: {e}")
            raise
    
    def update_record_status(self, event_id: str, status: str) -> bool:
        """レコードのステータスを更新"""
        try:
            existing_records = self._get_existing_records()
            
            if event_id not in existing_records:
                logger.warning(f"更新対象のレコードが見つかりません: {event_id}")
                return False
            
            row_number = existing_records[event_id]
            # ステータス列（L列）を更新
            self.worksheet.update(f'L{row_number}', status)
            
            logger.info(f"レコードのステータスを更新しました: {event_id} -> {status}")
            return True
            
        except Exception as e:
            logger.error(f"ステータスの更新に失敗しました: {event_id} - {e}")
            return False
    
    def delete_record(self, event_id: str) -> bool:
        """レコードを削除"""
        try:
            existing_records = self._get_existing_records()
            
            if event_id not in existing_records:
                logger.warning(f"削除対象のレコードが見つかりません: {event_id}")
                return False
            
            row_number = existing_records[event_id]
            self.worksheet.delete_rows(row_number)
            
            logger.info(f"レコードを削除しました: {event_id}")
            return True
            
        except Exception as e:
            logger.error(f"レコードの削除に失敗しました: {event_id} - {e}")
            return False
    
    def clear_all_records(self) -> bool:
        """全レコードを削除（ヘッダーは保持）"""
        try:
            # ヘッダー行を除く全行を削除
            all_values = self.worksheet.get_all_values()
            if len(all_values) > 1:
                self.worksheet.delete_rows(2, len(all_values))
                logger.info("全レコードを削除しました")
            
            return True
            
        except Exception as e:
            logger.error(f"全レコードの削除に失敗しました: {e}")
            return False
    
    def get_sheet_info(self) -> Dict[str, Any]:
        """シートの基本情報を取得"""
        try:
            if not self.worksheet:
                return {}
            
            # データ範囲を取得
            all_values = self.worksheet.get_all_values()
            
            info = {
                'title': self.worksheet.title,
                'row_count': len(all_values),
                'column_count': len(all_values[0]) if all_values else 0,
                'data_rows': len(all_values) - 1 if len(all_values) > 1 else 0,
                'last_updated': datetime.now().isoformat()
            }
            
            return info
            
        except Exception as e:
            logger.error(f"シート情報の取得に失敗しました: {e}")
            return {}
    
    def check_permissions(self) -> bool:
        """スプレッドシートへのアクセス権限をチェック"""
        try:
            if not self.worksheet:
                return False
            
            # 簡単な読み取り操作で権限をチェック
            self.worksheet.get_all_values()
            return True
            
        except APIError as e:
            if e.response.status_code == 403:
                logger.error("スプレッドシートへのアクセス権限がありません")
                return False
            else:
                logger.error(f"権限チェックでエラーが発生しました: {e}")
                return False
        except Exception as e:
            logger.error(f"権限チェックでエラーが発生しました: {e}")
            return False
    
    def batch_update(self, updates: List[Dict[str, Any]]) -> bool:
        """バッチ更新を実行"""
        try:
            if not updates:
                return True
            
            # バッチ更新用のリクエストを構築
            batch_requests = []
            
            for update in updates:
                if 'event_id' in update and 'values' in update:
                    existing_records = self._get_existing_records()
                    if update['event_id'] in existing_records:
                        row_number = existing_records[update['event_id']]
                        batch_requests.append({
                            'range': f'A{row_number}:N{row_number}',
                            'values': [update['values']]
                        })
            
            if batch_requests:
                self.worksheet.batch_update(batch_requests)
                logger.info(f"バッチ更新を実行しました: {len(batch_requests)}件")
            
            return True
            
        except Exception as e:
            logger.error(f"バッチ更新でエラーが発生しました: {e}")
            return False
