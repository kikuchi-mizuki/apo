"""
データモデルとスキーマ定義
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import json


class CalendarEvent(BaseModel):
    """Googleカレンダーイベントの基本モデル"""
    event_id: str
    title: str
    description: Optional[str] = None
    start: datetime
    end: datetime
    timezone: str
    attendees: List[Dict[str, str]] = Field(default_factory=list)
    organizer: Optional[Dict[str, str]] = None
    location: Optional[str] = None
    html_link: Optional[str] = None
    updated: datetime
    source_calendar: str

    @validator('start', 'end', pre=True)
    def parse_datetime(cls, v):
        """日時文字列をdatetimeオブジェクトに変換"""
        if isinstance(v, str):
            from dateutil import parser
            return parser.parse(v)
        return v


class ExtractedData(BaseModel):
    """AI抽出結果のモデル"""
    company_name: Optional[str] = None
    person_names: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    
    @validator('person_names', pre=True)
    def parse_person_names(cls, v):
        """文字列またはリストをリストに正規化"""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return [v] if v else []
        return v if isinstance(v, list) else []


class BookingRecord(BaseModel):
    """スプレッドシート出力用の予約記録モデル"""
    event_id: str
    title: str
    company_name: Optional[str] = None
    person_names: str = Field(default="[]")  # JSON文字列として保存
    start_datetime: datetime
    end_datetime: datetime
    timezone: str = "Asia/Tokyo"
    attendees: str = Field(default="[]")  # JSON文字列として保存
    location: Optional[str] = None
    source_calendar: str
    extracted_confidence: Optional[float] = None
    status: str = "active"  # active/removed/cancelled
    updated_at: datetime = Field(default_factory=datetime.now)
    run_id: Optional[str] = None  # 同期実行ID
    
    @validator('person_names', 'attendees', pre=True)
    def serialize_to_json(cls, v):
        """リストをJSON文字列に変換"""
        if isinstance(v, list):
            return json.dumps(v, ensure_ascii=False)
        elif isinstance(v, str):
            return v
        return "[]"
    
    @validator('start_datetime', 'end_datetime', pre=True)
    def ensure_timezone(cls, v):
        """タイムゾーンをAsia/Tokyoに正規化"""
        if isinstance(v, datetime):
            from dateutil import tz
            if v.tzinfo is None:
                # UTCとして扱い、Asia/Tokyoに変換
                utc_tz = tz.gettz('UTC')
                tokyo_tz = tz.gettz('Asia/Tokyo')
                v = v.replace(tzinfo=utc_tz)
                return v.astimezone(tokyo_tz)
            else:
                # 既存のタイムゾーンをAsia/Tokyoに変換
                tokyo_tz = tz.gettz('Asia/Tokyo')
                return v.astimezone(tokyo_tz)
        return v


class SyncResult(BaseModel):
    """同期実行結果のモデル"""
    run_id: str
    start_time: datetime
    end_time: datetime
    total_events: int
    matched_b_events: int
    upserted: int
    skipped: int
    errors: int
    error_details: List[str] = Field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        """実行時間（秒）"""
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_events == 0:
            return 0.0
        return (self.upserted + self.skipped) / self.total_events


class Config(BaseModel):
    """設定ファイルのモデル"""
    calendar: Dict[str, Any]
    event_filter: Dict[str, Any]
    ai_extraction: Dict[str, Any]
    spreadsheet: Dict[str, Any]
    logging: Dict[str, Any]
    sync: Dict[str, Any]
    notifications: Dict[str, Any]
