"""
データ正規化とバリデーション
"""
import re
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from loguru import logger

from .schemas import ExtractedData, BookingRecord


class DataNormalizer:
    """データの正規化とバリデーション"""
    
    def __init__(self):
        # 会社名の表記ゆれ辞書
        self.company_name_variations = {
            # 株式会社の表記ゆれ
            '㈱': '株式会社',
            '㈲': '有限会社',
            '㈳': '合同会社',
            '㈴': '一般社団法人',
            '㈵': '公益社団法人',
            '㈶': '一般財団法人',
            '㈷': '公益財団法人',
            '㈸': '社会福祉法人',
            '㈹': '学校法人',
            '㈺': '医療法人',
            '㈻': '宗教法人',
            '㈼': '特定非営利活動法人',
            
            # 英字の表記ゆれ
            'Inc': 'Inc.',
            'LLC': 'LLC',
            'Ltd': 'Ltd.',
            'Corp': 'Corp.',
            'Co': 'Co.',
        }
        
        # 人名の正規化ルール
        self.name_normalization_rules = [
            # 敬称の正規化
            (r'([一-龯]{2,4})様$', r'\1様'),
            (r'([一-龯]{2,4})さん$', r'\1さん'),
            (r'([一-龯]{2,4})氏$', r'\1氏'),
            (r'([一-龯]{2,4})殿$', r'\1殿'),
            
            # 全角・半角の正規化
            (r'　', ' '),  # 全角スペース → 半角スペース
            (r'（', '('),  # 全角括弧 → 半角括弧
            (r'）', ')'),
            (r'［', '['),  # 全角角括弧 → 半角角括弧
            (r'］', ']'),
        ]
    
    def normalize_extracted_data(self, extracted_data: ExtractedData) -> ExtractedData:
        """抽出データを正規化"""
        try:
            # 会社名の正規化
            normalized_company = self._normalize_company_name(extracted_data.company_name)
            
            # 人名の正規化
            normalized_person_names = self._normalize_person_names(extracted_data.person_names)
            
            # 重複除去
            normalized_person_names = list(set(normalized_person_names))
            
            return ExtractedData(
                company_name=normalized_company,
                person_names=normalized_person_names,
                confidence=extracted_data.confidence
            )
            
        except Exception as e:
            logger.error(f"抽出データの正規化でエラーが発生しました: {e}")
            return extracted_data
    
    def _normalize_company_name(self, company_name: Optional[str]) -> Optional[str]:
        """会社名を正規化"""
        if not company_name:
            return None
        
        normalized = company_name.strip()
        
        # 表記ゆれの正規化
        for variation, standard in self.company_name_variations.items():
            normalized = normalized.replace(variation, standard)
        
        # 前後の空白を除去
        normalized = normalized.strip()
        
        # 空文字列の場合はNone
        if not normalized:
            return None
        
        return normalized
    
    def _normalize_person_names(self, person_names: List[str]) -> List[str]:
        """人名を正規化"""
        if not person_names:
            return []
        
        normalized_names = []
        
        for name in person_names:
            if not name:
                continue
            
            normalized_name = name.strip()
            
            # 正規化ルールを適用
            for pattern, replacement in self.name_normalization_rules:
                normalized_name = re.sub(pattern, replacement, normalized_name)
            
            # 前後の空白を除去
            normalized_name = normalized_name.strip()
            
            # 有効な名前の場合のみ追加
            if normalized_name and self._is_valid_person_name(normalized_name):
                normalized_names.append(normalized_name)
        
        return normalized_names
    
    def _is_valid_person_name(self, name: str) -> bool:
        """有効な人名かどうかを判定"""
        if not name or len(name) < 2:
            return False
        
        # 明らかに無効なパターンを除外
        invalid_patterns = [
            r'^[0-9]+$',  # 数字のみ
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',  # メールアドレス
            r'^https?://',  # URL
            r'^[　\s]+$',  # 空白のみ
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, name):
                return False
        
        return True
    
    def validate_booking_record(self, record: BookingRecord) -> Dict[str, Any]:
        """予約記録のバリデーション"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 必須フィールドのチェック
            if not record.event_id:
                validation_result['is_valid'] = False
                validation_result['errors'].append('event_idが必須です')
            
            if not record.title:
                validation_result['is_valid'] = False
                validation_result['errors'].append('titleが必須です')
            
            if not record.start_datetime:
                validation_result['is_valid'] = False
                validation_result['errors'].append('start_datetimeが必須です')
            
            if not record.end_datetime:
                validation_result['is_valid'] = False
                validation_result['errors'].append('end_datetimeが必須です')
            
            # 日時の妥当性チェック
            if record.start_datetime and record.end_datetime:
                if record.start_datetime >= record.end_datetime:
                    validation_result['is_valid'] = False
                    validation_result['errors'].append('開始時刻は終了時刻より前である必要があります')
                
                # 過去の日時チェック（警告）
                now = datetime.now()
                if record.start_datetime < now:
                    validation_result['warnings'].append('開始時刻が過去の日時です')
            
            # 信頼度のチェック
            if record.extracted_confidence is not None:
                if record.extracted_confidence < 0.5:
                    validation_result['warnings'].append('抽出信頼度が低いです（0.5未満）')
            
            # ステータスの妥当性チェック
            valid_statuses = ['active', 'removed', 'cancelled']
            if record.status not in valid_statuses:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f'無効なステータスです: {record.status}')
            
            # JSONフィールドの妥当性チェック
            try:
                json.loads(record.person_names)
            except json.JSONDecodeError:
                validation_result['is_valid'] = False
                validation_result['errors'].append('person_namesが有効なJSONではありません')
            
            try:
                json.loads(record.attendees)
            except json.JSONDecodeError:
                validation_result['is_valid'] = False
                validation_result['errors'].append('attendeesが有効なJSONではありません')
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f'バリデーション中にエラーが発生しました: {e}')
        
        return validation_result
    
    def clean_company_name(self, company_name: str) -> str:
        """会社名のクリーニング"""
        if not company_name:
            return ""
        
        # 不要な文字を除去
        cleaned = re.sub(r'[^\w\s　\-\.\(\)（）［］【】]', '', company_name)
        
        # 前後の空白を除去
        cleaned = cleaned.strip()
        
        return cleaned
    
    def clean_person_name(self, person_name: str) -> str:
        """人名のクリーニング"""
        if not person_name:
            return ""
        
        # 不要な文字を除去（日本語文字、英数字、一部の記号を保持）
        cleaned = re.sub(r'[^\w\s　\-\.\(\)（）［］【】一-龯あ-んア-ン]', '', person_name)
        
        # 前後の空白を除去
        cleaned = cleaned.strip()
        
        return cleaned
    
    def merge_company_variations(self, company_names: List[str]) -> Dict[str, List[str]]:
        """会社名の表記ゆれをグループ化"""
        if not company_names:
            return {}
        
        # 正規化された会社名でグループ化
        groups = {}
        
        for company_name in company_names:
            if not company_name:
                continue
            
            normalized = self._normalize_company_name(company_name)
            if not normalized:
                continue
            
            if normalized not in groups:
                groups[normalized] = []
            
            if company_name not in groups[normalized]:
                groups[normalized].append(company_name)
        
        return groups
    
    def suggest_company_name(self, text: str) -> Optional[str]:
        """テキストから会社名を推測"""
        if not text:
            return None
        
        # 会社名のパターンを検索
        company_patterns = [
            r'([^　\s]+(?:株式会社|有限会社|合同会社|Inc\.|LLC|Ltd\.))',
            r'([^　\s]+(?:一般社団法人|公益社団法人|一般財団法人|公益財団法人))',
        ]
        
        for pattern in company_patterns:
            matches = re.findall(pattern, text)
            if matches:
                # 最初に見つかったものを返す
                return self._normalize_company_name(matches[0])
        
        return None
