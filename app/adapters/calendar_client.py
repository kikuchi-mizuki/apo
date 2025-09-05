"""
GoogleカレンダーAPIクライアント
"""
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

from ..core.schemas import CalendarEvent
from ..core.config import config_manager


class GoogleCalendarClient:
    """GoogleカレンダーAPIクライアント"""
    
    def __init__(self, credentials_path: Optional[str] = None):
        self.credentials_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        self.service = None
        self.calendar_id = None
        
        # 設定からカレンダーIDを取得
        calendar_config = config_manager.get_calendar_config()
        self.calendar_id = calendar_config.get('calendar_id', 'primary')
        
        # サービスを初期化
        self._initialize_service()
    
    def _initialize_service(self):
        """Googleカレンダーサービスを初期化"""
        try:
            service_account_info = os.getenv('GOOGLE_SERVICE_ACCOUNT_INFO')
            if service_account_info:
                # 環境変数のJSON本文から認証
                credentials = service_account.Credentials.from_service_account_info(
                    info=json.loads(service_account_info),
                    scopes=['https://www.googleapis.com/auth/calendar.readonly']
                )
                logger.info("サービスアカウント情報（環境変数）を使用")
            elif self.credentials_path and os.path.exists(self.credentials_path):
                # サービスアカウントキーのファイルを使用
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=['https://www.googleapis.com/auth/calendar.readonly']
                )
                logger.info(f"サービスアカウントキーを使用: {self.credentials_path}")
            else:
                # 環境変数から認証情報を取得
                credentials = Credentials.from_authorized_user_info(
                    info={
                        'client_id': os.getenv('GOOGLE_CLIENT_ID'),
                        'client_secret': os.getenv('GOOGLE_CLIENT_SECRET'),
                        'refresh_token': os.getenv('GOOGLE_REFRESH_TOKEN'),
                    },
                    scopes=['https://www.googleapis.com/auth/calendar.readonly']
                )
                logger.info("環境変数から認証情報を取得")
            
            # 認証情報を更新
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            
            # サービスを構築
            self.service = build('calendar', 'v3', credentials=credentials)
            logger.info("Googleカレンダーサービスを初期化しました")
            
        except Exception as e:
            logger.error(f"Googleカレンダーサービスの初期化に失敗しました: {e}")
            raise
    
    def get_events(self, start_date: Optional[datetime] = None, 
                   end_date: Optional[datetime] = None,
                   max_results: Optional[int] = None) -> List[CalendarEvent]:
        """指定期間のイベントを取得"""
        try:
            if not self.service:
                raise RuntimeError("カレンダーサービスが初期化されていません")
            
            # デフォルトの期間を設定
            if not start_date:
                calendar_config = config_manager.get_calendar_config()
                past_days = calendar_config.get('sync_window_past_days', 30)
                start_date = datetime.now() - timedelta(days=past_days)
            
            if not end_date:
                calendar_config = config_manager.get_calendar_config()
                future_days = calendar_config.get('sync_window_future_days', 60)
                end_date = datetime.now() + timedelta(days=future_days)
            
            # 最大取得件数を設定
            if not max_results:
                calendar_config = config_manager.get_calendar_config()
                max_results = calendar_config.get('max_results', 1000)
            
            logger.info(f"イベント取得開始: {start_date} 〜 {end_date}")
            
            # イベントを取得
            events_result = self.service.events().list(
                calendarId=self.calendar_id,
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                maxResults=max_results,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            logger.info(f"イベント取得完了: {len(events)}件")
            
            # CalendarEventオブジェクトに変換
            calendar_events = []
            for event in events:
                try:
                    calendar_event = self._convert_to_calendar_event(event)
                    if calendar_event:
                        calendar_events.append(calendar_event)
                except Exception as e:
                    logger.warning(f"イベントの変換に失敗しました: {event.get('id', 'unknown')} - {e}")
                    continue
            
            return calendar_events
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.error("カレンダーへのアクセス権限がありません")
            elif e.resp.status == 404:
                logger.error("カレンダーが見つかりません")
            else:
                logger.error(f"カレンダーAPIエラー: {e}")
            raise
        except Exception as e:
            logger.error(f"イベント取得でエラーが発生しました: {e}")
            raise
    
    def _convert_to_calendar_event(self, event_data: Dict[str, Any]) -> Optional[CalendarEvent]:
        """GoogleカレンダーイベントをCalendarEventに変換"""
        try:
            # 開始時刻と終了時刻を取得
            start_data = event_data.get('start', {})
            end_data = event_data.get('end', {})
            
            if not start_data or not end_data:
                logger.warning(f"開始時刻または終了時刻がありません: {event_data.get('id', 'unknown')}")
                return None
            
            # 日時を解析
            start_datetime = self._parse_datetime(start_data)
            end_datetime = self._parse_datetime(end_data)
            
            if not start_datetime or not end_datetime:
                logger.warning(f"日時の解析に失敗しました: {event_data.get('id', 'unknown')}")
                return None
            
            # 出席者情報を取得
            attendees = []
            if 'attendees' in event_data:
                for attendee in event_data['attendees']:
                    attendee_info = {}
                    if 'displayName' in attendee:
                        attendee_info['displayName'] = attendee['displayName']
                    if 'email' in attendee:
                        attendee_info['email'] = attendee['email']
                    attendees.append(attendee_info)
            
            # 主催者情報を取得
            organizer = None
            if 'organizer' in event_data:
                org_raw = event_data.get('organizer') or {}
                org_obj: Dict[str, str] = {}
                display_name = org_raw.get('displayName')
                email = org_raw.get('email')
                if isinstance(display_name, str):
                    org_obj['displayName'] = display_name
                if isinstance(email, str):
                    org_obj['email'] = email
                if org_obj:
                    organizer = org_obj
            
            return CalendarEvent(
                event_id=event_data['id'],
                title=event_data.get('summary', ''),
                description=event_data.get('description'),
                start=start_datetime,
                end=end_datetime,
                timezone=start_data.get('timeZone', 'UTC'),
                attendees=attendees,
                organizer=organizer,
                location=event_data.get('location'),
                html_link=event_data.get('htmlLink'),
                updated=datetime.fromisoformat(event_data['updated'].replace('Z', '+00:00')),
                source_calendar=self.calendar_id
            )
            
        except Exception as e:
            logger.error(f"イベント変換でエラーが発生しました: {e}")
            return None
    
    def _parse_datetime(self, datetime_data: Dict[str, Any]) -> Optional[datetime]:
        """日時データを解析"""
        try:
            if 'dateTime' in datetime_data:
                # 日時指定の場合
                dt_str = datetime_data['dateTime']
                if dt_str.endswith('Z'):
                    dt_str = dt_str[:-1] + '+00:00'
                return datetime.fromisoformat(dt_str)
            elif 'date' in datetime_data:
                # 日付のみ指定の場合（終日イベント）
                date_str = datetime_data['date']
                return datetime.fromisoformat(date_str)
            else:
                return None
        except Exception as e:
            logger.error(f"日時解析でエラーが発生しました: {e}")
            return None
    
    def get_event_by_id(self, event_id: str) -> Optional[CalendarEvent]:
        """指定IDのイベントを取得"""
        try:
            if not self.service:
                raise RuntimeError("カレンダーサービスが初期化されていません")
            
            event_result = self.service.events().get(
                calendarId=self.calendar_id,
                eventId=event_id
            ).execute()
            
            return self._convert_to_calendar_event(event_result)
            
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"イベントが見つかりません: {event_id}")
                return None
            else:
                logger.error(f"イベント取得でエラーが発生しました: {e}")
                raise
        except Exception as e:
            logger.error(f"イベント取得でエラーが発生しました: {e}")
            raise
    
    def list_calendars(self) -> List[Dict[str, str]]:
        """利用可能なカレンダーの一覧を取得"""
        try:
            if not self.service:
                raise RuntimeError("カレンダーサービスが初期化されていません")
            
            calendar_list = self.service.calendarList().list().execute()
            calendars = []
            
            for calendar in calendar_list.get('items', []):
                calendars.append({
                    'id': calendar['id'],
                    'summary': calendar.get('summary', ''),
                    'description': calendar.get('description', ''),
                    'accessRole': calendar.get('accessRole', '')
                })
            
            return calendars
            
        except Exception as e:
            logger.error(f"カレンダー一覧の取得でエラーが発生しました: {e}")
            raise
    
    def check_permissions(self) -> bool:
        """カレンダーへのアクセス権限をチェック"""
        try:
            if not self.service:
                return False
            
            # 簡単なAPI呼び出しで権限をチェック
            self.service.calendars().get(calendarId=self.calendar_id).execute()
            return True
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.error("カレンダーへのアクセス権限がありません")
                return False
            else:
                logger.error(f"権限チェックでエラーが発生しました: {e}")
                return False
        except Exception as e:
            logger.error(f"権限チェックでエラーが発生しました: {e}")
            return False
    
    def get_calendar_info(self) -> Optional[Dict[str, Any]]:
        """カレンダーの基本情報を取得"""
        try:
            if not self.service:
                return None
            
            calendar_info = self.service.calendars().get(calendarId=self.calendar_id).execute()
            
            return {
                'id': calendar_info['id'],
                'summary': calendar_info.get('summary', ''),
                'description': calendar_info.get('description', ''),
                'timeZone': calendar_info.get('timeZone', ''),
                'accessRole': calendar_info.get('accessRole', '')
            }
            
        except Exception as e:
            logger.error(f"カレンダー情報の取得でエラーが発生しました: {e}")
            return None
