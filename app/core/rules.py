"""
規則ベースの抽出ロジック
"""
import re
import json
from typing import List, Optional, Dict, Any, Tuple
from rapidfuzz import fuzz, process
from loguru import logger

from .schemas import ExtractedData


class RuleBasedExtractor:
    """規則ベースの会社名・人名抽出器"""
    
    def __init__(self):
        # 会社名の接尾語パターン
        self.company_suffixes = [
            '株式会社', '有限会社', '合同会社', '一般社団法人', '公益社団法人',
            '一般財団法人', '公益財団法人', '社会福祉法人', '学校法人',
            '医療法人', '宗教法人', '特定非営利活動法人',
            'Inc.', 'LLC', 'Ltd.', 'Corp.', 'Corporation',
            'Co.', 'Company', 'Limited'
        ]
        
        # 日本語人名のパターン
        self.japanese_name_patterns = [
            r'^[一-龯]{2,4}$',  # 漢字2-4文字
            r'^[あ-ん]{2,6}$',  # ひらがな2-6文字
            r'^[ア-ン]{2,6}$',  # カタカナ2-6文字
        ]
        
        # メールドメイン→企業名辞書（例）
        self.domain_company_map = {
            'example.co.jp': 'Example株式会社',
            'sample.com': 'Sample Inc.',
            # 実際の運用では、より多くのマッピングを追加
        }
        
        # 既存の会社名辞書（実際の運用では、スプレッドシートから動的に取得）
        self.existing_companies = set()
    
    def extract_from_event(self, event_data: Dict[str, Any]) -> ExtractedData:
        """イベントデータから会社名・人名を抽出"""
        try:
            # テキストデータを収集
            text_data = self._collect_text_data(event_data)
            
            # 会社名を抽出
            company_name = self._extract_company_name(text_data)
            
            # 人名を抽出
            person_names = self._extract_person_names(text_data, event_data)
            
            # 信頼度を計算
            confidence = self._calculate_confidence(company_name, person_names, text_data)
            
            return ExtractedData(
                company_name=company_name,
                person_names=person_names,
                confidence=confidence
            )
            
        except Exception as e:
            logger.error(f"規則ベース抽出でエラーが発生しました: {e}")
            return ExtractedData(confidence=0.0)
    
    def _collect_text_data(self, event_data: Dict[str, Any]) -> str:
        """イベントからテキストデータを収集"""
        text_parts = []
        
        # タイトル
        if event_data.get('title'):
            title = event_data['title']
            # 先頭の【B】を除去
            title = re.sub(r'^[\s　]*【B】', '', title)
            text_parts.append(title)
            # タイトルから会社名の強制抽出候補（先頭セグメント）
            forced = self._extract_company_from_title(title)
            if forced:
                text_parts.append(forced)
        
        # 説明
        if event_data.get('description'):
            text_parts.append(event_data['description'])
        
        # 場所
        if event_data.get('location'):
            text_parts.append(event_data['location'])
        
        # 出席者名
        attendees = event_data.get('attendees', [])
        for attendee in attendees:
            if attendee.get('displayName'):
                text_parts.append(attendee['displayName'])
        
        return ' '.join(text_parts)
    
    def _extract_company_name(self, text_data: str) -> Optional[str]:
        """テキストから会社名を抽出"""
        if not text_data:
            return None
        
        # タイトル先頭パートからの抽出を最優先
        title_based = self._extract_company_from_title(text_data)
        if title_based:
            return title_based

        # 1. 接尾語パターンで会社名を検索
        company_candidates = self._find_companies_by_suffix(text_data)
        
        # 2. メールドメインから会社名を推定
        domain_company = self._extract_company_from_domain(text_data)
        if domain_company:
            company_candidates.append(domain_company)
        
        # 3. 既存の会社名との照合
        existing_match = self._find_existing_company_match(text_data)
        if existing_match:
            company_candidates.append(existing_match)
        
        # 4. 最適な候補を選択
        if company_candidates:
            # 信頼度の高いものを優先
            return self._select_best_company_candidate(company_candidates, text_data)
        
        return None
    
    def _find_companies_by_suffix(self, text: str) -> List[str]:
        """接尾語パターンで会社名を検索"""
        companies = []
        
        for suffix in self.company_suffixes:
            # 接尾語の前の部分を抽出
            pattern = rf'([^　\s]+{re.escape(suffix)})'
            matches = re.findall(pattern, text)
            companies.extend(matches)
        
        return companies
    
    def _extract_company_from_domain(self, text: str) -> Optional[str]:
        """メールドメインから会社名を推定"""
        # メールアドレスを検索
        email_pattern = r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        emails = re.findall(email_pattern, text)
        
        for email in emails:
            domain = email.split('@')[1]
            if domain in self.domain_company_map:
                return self.domain_company_map[domain]
        
        return None
    
    def _find_existing_company_match(self, text: str) -> Optional[str]:
        """既存の会社名との照合"""
        if not self.existing_companies:
            return None
        
        # ファジー照合で最適なマッチを検索
        best_match = process.extractOne(
            text, 
            self.existing_companies, 
            scorer=fuzz.token_sort_ratio,
            score_cutoff=80
        )
        
        return best_match[0] if best_match else None
    
    def _select_best_company_candidate(self, candidates: List[str], context: str) -> str:
        """最適な会社名候補を選択"""
        if not candidates:
            return None
        
        if len(candidates) == 1:
            return candidates[0]
        
        # 複数候補がある場合、コンテキストとの関連性でスコアリング
        best_candidate = None
        best_score = 0
        
        for candidate in candidates:
            # コンテキスト内での出現頻度
            frequency = context.count(candidate)
            
            # 長さ（適度な長さを好む）
            length_score = min(len(candidate) / 20, 1.0)
            
            # 総合スコア
            score = frequency * 0.6 + length_score * 0.4
            
            if score > best_score:
                best_score = score
                best_candidate = candidate
        
        return best_candidate
    
    def _extract_person_names(self, text_data: str, event_data: Dict[str, Any]) -> List[str]:
        """テキストから人名を抽出"""
        person_names = set()
        
        # 1. 出席者名から抽出（最優先）
        attendees = event_data.get('attendees', [])
        for attendee in attendees:
            if attendee.get('displayName'):
                name = attendee['displayName'].strip()
                if self._is_valid_person_name(name):
                    person_names.add(name)
        
        # 2. テキストから人名パターンを抽出
        text_names = self._extract_names_from_text(text_data)
        person_names.update(text_names)
        
        # 3. 重複を除去し、リストに変換
        return list(person_names)
    
    def _extract_names_from_text(self, text: str) -> List[str]:
        """テキストから人名パターンを抽出"""
        names = []
        
        # 日本語人名のパターンを検索
        for pattern in self.japanese_name_patterns:
            matches = re.findall(pattern, text)
            names.extend(matches)
        
        # 一般的な人名パターン（例：田中様、山田さん）
        # 注意: 以前の実装では文字クラス [様さん] により 1 文字のみ一致し、
        # 「さん」が「さ」になる不具合があったため (様|さん) に修正
        honorific_pattern = r'([一-龯]{1,4}(様|さん))'
        honorific_matches = re.findall(honorific_pattern, text)
        # re.findall でグループがあるため、最初のグループだけを取り出す
        for m in honorific_matches:
            names.append(m[0])

        # 先生/氏 などの敬称
        honorific2_pattern = r'([一-龯]{1,4}(先生|氏))'
        for m in re.findall(honorific2_pattern, text):
            names.append(m[0])

        # 姓 名 形式（スペース区切り）の簡易検出（例：菊池 瑞貴）
        spaced_fullname = r'([一-龯]{1,3}[　\s][一-龯]{1,3})(?:様|さん|先生|氏)?'
        for m in re.findall(spaced_fullname, text):
            names.append(m)

        # ひらがな/カタカナ + 敬称（例：さとうさん／タナカさん）
        kana_honorific = r'([ぁ-んァ-ヶー]{2,10}(?:さん|様))'
        for m in re.findall(kana_honorific, text):
            names.append(m)
        
        return names

    def _extract_company_from_title(self, title: str) -> Optional[str]:
        """タイトルの先頭セグメントから会社名っぽい文字列を抽出"""
        if not title:
            return None
        # デリミタで分割
        head = re.split(r'[\|/／・×x\-—~〜]', title, maxsplit=1)[0]
        head = head.strip()
        if not head:
            return None
        
        # 会社接尾語が含まれていればそのまま返す
        if any(suf in head for suf in self.company_suffixes):
            return head
        
        # よくある会社語尾（商事, 工業, 製作所 など）
        generic_terms = ['商事', '工業', '製作所', '不動産', '銀行', '信用金庫', 'センター', '研究所']
        if any(term in head for term in generic_terms):
            return head
        
        # 新しいパターン: 会社名っぽい文字列の判定を強化
        # 1. カタカナ + プラス/plus/plusなどの組み合わせ
        if re.match(r'^[ァ-ヶー]+(プラス|plus|Plus)', head, re.IGNORECASE):
            return head
        
        # 2. ひらがな + プラス/plus/plusなどの組み合わせ
        if re.match(r'^[あ-ん]+(プラス|plus|Plus)', head, re.IGNORECASE):
            return head
        
        # 3. 漢字 + プラス/plus/plusなどの組み合わせ
        if re.match(r'^[一-龯]+(プラス|plus|Plus)', head, re.IGNORECASE):
            return head
        
        # 4. 一般的な会社名パターン（3文字以上、特定の語尾）
        company_endings = ['プラス', 'plus', 'Plus', 'サービス', 'service', 'Service', 
                          'ソリューション', 'solution', 'Solution', 'クリニック', 'clinic', 'Clinic',
                          'クリニック', 'クリニック', 'クリニック', 'クリニック', 'クリニック']
        for ending in company_endings:
            if head.endswith(ending) and len(head) >= 3:
                return head
        
        # 5. カタカナのみの3文字以上の文字列（会社名の可能性）
        if re.match(r'^[ァ-ヶー]{3,}$', head):
            return head
        
        # 6. ひらがなのみの3文字以上の文字列（会社名の可能性）
        if re.match(r'^[あ-ん]{3,}$', head):
            return head
        
        return None
    
    def _is_valid_person_name(self, name: str) -> bool:
        """有効な人名かどうかを判定"""
        if not name or len(name) < 2:
            return False
        
        # 明らかに会社名っぽいものを除外
        for suffix in self.company_suffixes:
            if suffix in name:
                return False

        # 役職/職種などのNGワードを除外
        ng_terms = ['コーチング', 'コーチ', 'セラピスト', 'カウンセラー', '面談', '商談', '打合せ', '打ち合わせ', 'ミーティング']
        for term in ng_terms:
            if term in name:
                return False
        
        # メールアドレスを除外
        if '@' in name:
            return False
        
        # 日本語人名パターンに一致するかチェック
        for pattern in self.japanese_name_patterns:
            if re.match(pattern, name):
                return True
        
        # 敬称付きの名前（様/さん/先生/氏）
        if re.match(r'[一-龯ぁ-んァ-ヶー]{1,10}(様|さん|先生|氏)$', name):
            return True
        
        return False
    
    def _calculate_confidence(self, company_name: Optional[str], 
                            person_names: List[str], 
                            text_data: str) -> float:
        """抽出結果の信頼度を計算"""
        confidence = 0.0
        
        # 会社名の信頼度
        if company_name:
            # 接尾語がある場合
            if any(suffix in company_name for suffix in self.company_suffixes):
                confidence += 0.4
            # 既存の会社名とマッチした場合
            elif company_name in self.existing_companies:
                confidence += 0.3
            # メールドメインから推定した場合
            elif any(domain in text_data for domain in self.domain_company_map):
                confidence += 0.2
            else:
                confidence += 0.1
        
        # 人名の信頼度
        if person_names:
            # 出席者名から抽出した場合
            confidence += 0.3
            # テキストから抽出した場合
            if len(person_names) > 1:
                confidence += 0.2
            else:
                confidence += 0.1
        
        # テキストの豊富さ
        if len(text_data) > 50:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def update_existing_companies(self, companies: List[str]):
        """既存の会社名辞書を更新"""
        self.existing_companies.update(companies)
        logger.info(f"既存の会社名辞書を更新しました: {len(companies)}件")
    
    def add_domain_company_mapping(self, domain: str, company: str):
        """ドメイン→会社名マッピングを追加"""
        self.domain_company_map[domain] = company
        logger.info(f"ドメイン→会社名マッピングを追加: {domain} -> {company}")
