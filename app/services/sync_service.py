"""
Googleカレンダー→AI抽出→スプレッドシート連携の同期サービス
"""
import re
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger

from ..core.schemas import CalendarEvent, BookingRecord, ExtractedData, SyncResult
from ..core.config import config_manager
from ..core.rules import RuleBasedExtractor
from ..core.extractor import AIExtractor, HybridExtractor
from ..core.normalizer import DataNormalizer
from ..adapters.calendar_client import GoogleCalendarClient
from ..adapters.sheets_client import GoogleSheetsClient


class CalendarSyncService:
    """カレンダー同期サービス"""
    
    def __init__(self):
        # 設定を取得
        self.config = config_manager.config
        self.event_filter_config = config_manager.get_event_filter_config()
        self.ai_config = config_manager.get_ai_extraction_config()
        
        # 正規表現パターンを初期化
        self.b_event_pattern = re.compile(self.event_filter_config.get('b_event_pattern', r'^[　\s]*【B】'))
        
        # 抽出器を初期化
        self.rule_extractor = RuleBasedExtractor()
        self.ai_extractor = AIExtractor(
            provider=self.ai_config.get('provider', 'openai'),
            model=self.ai_config.get('model', 'gpt-4o-mini'),
            api_key=config_manager.config.ai_extraction.get('api_key')
        )
        self.hybrid_extractor = HybridExtractor(self.rule_extractor, self.ai_extractor)
        
        # 正規化器を初期化
        self.normalizer = DataNormalizer()
        
        # クライアントを初期化
        self.calendar_client = GoogleCalendarClient()
        self.sheets_client = GoogleSheetsClient()
        
        # 信頼度閾値を設定
        confidence_threshold = self.ai_config.get('confidence_threshold', 0.8)
        self.hybrid_extractor.set_confidence_threshold(confidence_threshold)
        
        logger.info("カレンダー同期サービスを初期化しました")
    
    def sync_calendar_to_sheets(self, start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               run_id: Optional[str] = None) -> SyncResult:
        """カレンダーからスプレッドシートへの同期を実行"""
        if not run_id:
            run_id = str(uuid.uuid4())
        
        sync_result = SyncResult(
            run_id=run_id,
            start_time=datetime.now(),
            end_time=datetime.now(),
            total_events=0,
            matched_b_events=0,
            upserted=0,
            skipped=0,
            errors=0,
            error_details=[]
        )
        
        try:
            logger.info(f"同期開始: {run_id}")
            
            # 1. カレンダーからイベントを取得
            events = self._fetch_calendar_events(start_date, end_date)
            sync_result.total_events = len(events)
            logger.info(f"カレンダーから{len(events)}件のイベントを取得")
            
            # 2. 【B】イベントをフィルタリング
            b_events = self._filter_b_events(events)
            sync_result.matched_b_events = len(b_events)
            logger.info(f"【B】イベント: {len(b_events)}件")
            
            # 3. 各イベントを処理
            booking_records = []
            for event in b_events:
                try:
                    record = self._process_single_event(event, run_id)
                    if record:
                        booking_records.append(record)
                except Exception as e:
                    error_msg = f"イベント処理エラー: {event.event_id} - {e}"
                    logger.error(error_msg)
                    sync_result.error_details.append(error_msg)
                    sync_result.errors += 1
            
            # 4. スプレッドシートにupsert
            if booking_records:
                upsert_result = self.sheets_client.upsert_booking_records(booking_records)
                sync_result.upserted = upsert_result['upserted']
                sync_result.errors += upsert_result['errors']
            
            # 5. 既存の会社名辞書を更新
            self._update_company_dictionary()
            
            sync_result.end_time = datetime.now()
            sync_result.skipped = sync_result.total_events - sync_result.matched_b_events
            
            logger.info(f"同期完了: {run_id}")
            logger.info(f"結果: 総数{sync_result.total_events}, 【B】{sync_result.matched_b_events}, "
                       f"upsert{sync_result.upserted}, エラー{sync_result.errors}")
            
            return sync_result
            
        except Exception as e:
            error_msg = f"同期処理でエラーが発生しました: {e}"
            logger.error(error_msg)
            sync_result.error_details.append(error_msg)
            sync_result.errors += 1
            sync_result.end_time = datetime.now()
            return sync_result
    
    def _fetch_calendar_events(self, start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> List[CalendarEvent]:
        """カレンダーからイベントを取得"""
        try:
            return self.calendar_client.get_events(start_date, end_date)
        except Exception as e:
            logger.error(f"カレンダーからのイベント取得に失敗しました: {e}")
            raise
    
    def _filter_b_events(self, events: List[CalendarEvent]) -> List[CalendarEvent]:
        """【B】イベントをフィルタリング"""
        b_events = []
        
        for event in events:
            if self._is_b_event(event.title):
                b_events.append(event)
            else:
                logger.debug(f"【B】イベントではありません: {event.title}")
        
        return b_events
    
    def _is_b_event(self, title: str) -> bool:
        """タイトルが【B】イベントかどうかを判定"""
        if not title:
            return False
        
        return bool(self.b_event_pattern.match(title))
    
    def _process_single_event(self, event: CalendarEvent, run_id: str) -> Optional[BookingRecord]:
        """単一イベントを処理"""
        try:
            # 1. AI抽出を実行
            extracted_data = self.hybrid_extractor.extract_from_event(event)
            
            # 2. 抽出結果を正規化
            normalized_data = self.normalizer.normalize_extracted_data(extracted_data)
            
            # 3. BookingRecordを作成
            booking_record = BookingRecord(
                event_id=event.event_id,
                title=event.title,
                company_name=normalized_data.company_name,
                person_names=normalized_data.person_names,
                start_datetime=event.start,
                end_datetime=event.end,
                timezone=event.timezone,
                attendees=event.attendees,
                location=event.location,
                source_calendar=event.source_calendar,
                extracted_confidence=normalized_data.confidence,
                status='active',
                updated_at=datetime.now(),
                run_id=run_id
            )
            
            # 4. バリデーション
            validation_result = self.normalizer.validate_booking_record(booking_record)
            if not validation_result['is_valid']:
                logger.warning(f"バリデーションエラー: {event.event_id} - {validation_result['errors']}")
                if validation_result['warnings']:
                    logger.info(f"警告: {event.event_id} - {validation_result['warnings']}")
            
            return booking_record
            
        except Exception as e:
            logger.error(f"イベント処理でエラーが発生しました: {event.event_id} - {e}")
            return None
    
    def _update_company_dictionary(self):
        """既存の会社名辞書を更新"""
        try:
            # スプレッドシートから既存の会社名を取得
            existing_records = self.sheets_client.get_booking_records()
            
            company_names = set()
            for record in existing_records:
                if record.get('company_name'):
                    company_names.add(record['company_name'])
            
            # 規則ベース抽出器の辞書を更新
            self.rule_extractor.update_existing_companies(list(company_names))
            
            logger.info(f"会社名辞書を更新しました: {len(company_names)}件")
            
        except Exception as e:
            logger.warning(f"会社名辞書の更新に失敗しました: {e}")
    
    def get_sync_status(self) -> Dict[str, Any]:
        """同期状況を取得"""
        try:
            # カレンダー情報
            calendar_info = self.calendar_client.get_calendar_info()
            calendar_permissions = self.calendar_client.check_permissions()
            
            # スプレッドシート情報
            sheet_info = self.sheets_client.get_sheet_info()
            sheet_permissions = self.sheets_client.check_permissions()
            
            # 設定情報
            config_info = {
                'calendar_id': self.calendar_client.calendar_id,
                'sheet_name': self.sheets_client.sheet_name,
                'b_event_pattern': self.event_filter_config.get('b_event_pattern'),
                'confidence_threshold': self.ai_config.get('confidence_threshold'),
                'ai_provider': self.ai_config.get('provider'),
                'ai_model': self.ai_config.get('model')
            }
            
            return {
                'status': 'ready' if calendar_permissions and sheet_permissions else 'error',
                'calendar': {
                    'info': calendar_info,
                    'permissions': calendar_permissions
                },
                'spreadsheet': {
                    'info': sheet_info,
                    'permissions': sheet_permissions
                },
                'config': config_info,
                'last_check': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"同期状況の取得でエラーが発生しました: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
    
    def cleanup_old_records(self, days_to_keep: int = 90) -> Dict[str, int]:
        """古いレコードをクリーンアップ"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # 古いレコードを取得
            existing_records = self.sheets_client.get_booking_records()
            
            records_to_remove = []
            for record in existing_records:
                try:
                    # updated_atを解析
                    updated_at_str = record.get('updated_at', '')
                    if updated_at_str:
                        updated_at = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
                        if updated_at < cutoff_date:
                            records_to_remove.append(record['event_id'])
                except Exception as e:
                    logger.warning(f"日時の解析に失敗しました: {record.get('event_id', 'unknown')} - {e}")
            
            # 古いレコードを削除
            removed_count = 0
            for event_id in records_to_remove:
                if self.sheets_client.delete_record(event_id):
                    removed_count += 1
            
            logger.info(f"古いレコードのクリーンアップ完了: {removed_count}件削除")
            return {'removed': removed_count, 'total_checked': len(existing_records)}
            
        except Exception as e:
            logger.error(f"古いレコードのクリーンアップでエラーが発生しました: {e}")
            return {'removed': 0, 'total_checked': 0, 'error': str(e)}
    
    def export_data(self, format_type: str = 'csv') -> str:
        """データをエクスポート"""
        try:
            if format_type.lower() == 'csv':
                return self._export_to_csv()
            elif format_type.lower() == 'json':
                return self._export_to_json()
            else:
                raise ValueError(f"サポートされていないフォーマット: {format_type}")
                
        except Exception as e:
            logger.error(f"データエクスポートでエラーが発生しました: {e}")
            raise
    
    def _export_to_csv(self) -> str:
        """CSV形式でエクスポート"""
        try:
            records = self.sheets_client.get_booking_records()
            
            if not records:
                return ""
            
            # ヘッダー行
            headers = list(records[0].keys())
            csv_lines = [','.join(headers)]
            
            # データ行
            for record in records:
                row_values = []
                for header in headers:
                    value = record.get(header, '')
                    # CSVエスケープ
                    if ',' in str(value) or '"' in str(value):
                        value = f'"{str(value).replace('"', '""')}"'
                    row_values.append(str(value))
                csv_lines.append(','.join(row_values))
            
            return '\n'.join(csv_lines)
            
        except Exception as e:
            logger.error(f"CSVエクスポートでエラーが発生しました: {e}")
            raise
    
    def _export_to_json(self) -> str:
        """JSON形式でエクスポート"""
        try:
            records = self.sheets_client.get_booking_records()
            return json.dumps(records, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"JSONエクスポートでエラーが発生しました: {e}")
            raise
