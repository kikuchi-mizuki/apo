"""
設定管理クラス
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from loguru import logger

from .schemas import Config


class ConfigManager:
    """設定ファイルと環境変数の管理"""
    
    def __init__(self, config_path: str = "config.yaml", env_path: str = ".env"):
        self.config_path = Path(config_path)
        self.env_path = Path(env_path)
        self._config: Optional[Config] = None
        
        # 環境変数を読み込み
        self._load_env()
        
        # 設定ファイルを読み込み
        self._load_config()
        
        # ログ設定を適用
        self._setup_logging()
    
    def _load_env(self):
        """環境変数ファイルを読み込み"""
        if self.env_path.exists():
            load_dotenv(self.env_path)
            logger.info(f"環境変数ファイルを読み込みました: {self.env_path}")
        else:
            logger.warning(f"環境変数ファイルが見つかりません: {self.env_path}")
    
    def _load_config(self):
        """設定ファイルを読み込み"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"設定ファイルが見つかりません: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # 環境変数で上書き
            config_data = self._override_with_env(config_data)
            
            self._config = Config(**config_data)
            logger.info(f"設定ファイルを読み込みました: {self.config_path}")
            
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗しました: {e}")
            raise
    
    def _override_with_env(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """環境変数で設定値を上書き"""
        env_mappings = {
            'GOOGLE_CALENDAR_ID': ('calendar', 'calendar_id'),
            'SYNC_WINDOW_PAST_DAYS': ('calendar', 'sync_window_past_days'),
            'SYNC_WINDOW_FUTURE_DAYS': ('calendar', 'sync_window_future_days'),
            'OPENAI_API_KEY': ('ai_extraction', 'api_key'),
            'LOG_LEVEL': ('logging', 'level'),
            'LOG_FILE': ('logging', 'log_file'),
            'SYNC_INTERVAL_MINUTES': ('sync', 'interval_minutes'),
            'SLACK_WEBHOOK_URL': ('notifications', 'slack_webhook_url'),
        }
        
        for env_key, config_path in env_mappings.items():
            env_value = os.getenv(env_key)
            if env_value is not None:
                # ネストした辞書のパスを解決
                current = config_data
                for key in config_path[:-1]:
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                
                # 値を設定（型変換も行う）
                key = config_path[-1]
                if key in ['sync_window_past_days', 'sync_window_future_days', 'interval_minutes']:
                    current[key] = int(env_value)
                elif key in ['extracted_confidence']:
                    current[key] = float(env_value)
                else:
                    current[key] = env_value
                
                logger.debug(f"環境変数で上書き: {env_key} -> {config_path}")
        
        return config_data
    
    def _setup_logging(self):
        """ログ設定を適用"""
        if not self._config:
            return
        
        log_config = self._config.logging
        
        # 既存のログハンドラーをクリア
        logger.remove()
        
        # コンソール出力
        logger.add(
            lambda msg: print(msg, end=""),
            level=log_config.get('level', 'INFO'),
            format=log_config.get('format', '{time} | {level} | {message}'),
            colorize=True
        )
        
        # ファイル出力（設定されている場合）
        if log_config.get('file_output', False):
            log_file = log_config.get('log_file', 'logs/sync.log')
            log_dir = Path(log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            
            logger.add(
                log_file,
                level=log_config.get('level', 'INFO'),
                format=log_config.get('format', '{time} | {level} | {message}'),
                rotation="1 day",
                retention="30 days",
                encoding="utf-8"
            )
    
    @property
    def config(self) -> Config:
        """設定オブジェクトを取得"""
        if not self._config:
            raise RuntimeError("設定が読み込まれていません")
        return self._config
    
    def get_calendar_config(self) -> Dict[str, Any]:
        """カレンダー設定を取得"""
        return self.config.calendar
    
    def get_event_filter_config(self) -> Dict[str, Any]:
        """イベントフィルタ設定を取得"""
        return self.config.event_filter
    
    def get_ai_extraction_config(self) -> Dict[str, Any]:
        """AI抽出設定を取得"""
        return self.config.ai_extraction
    
    def get_spreadsheet_config(self) -> Dict[str, Any]:
        """スプレッドシート設定を取得"""
        return self.config.spreadsheet
    
    def get_sync_config(self) -> Dict[str, Any]:
        """同期設定を取得"""
        return self.config.sync
    
    def get_notification_config(self) -> Dict[str, Any]:
        """通知設定を取得"""
        return self.config.notifications
    
    def reload(self):
        """設定を再読み込み"""
        logger.info("設定を再読み込みします")
        self._load_config()
        self._setup_logging()


# グローバル設定インスタンス
config_manager = ConfigManager()
