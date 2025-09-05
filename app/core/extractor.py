"""
AI抽出とハイブリッド抽出のロジック
"""
import json
import re
from typing import List, Optional, Dict, Any
from loguru import logger

from .schemas import ExtractedData, CalendarEvent
from .rules import RuleBasedExtractor


class AIExtractor:
    """LLMを使用したAI抽出器"""
    
    def __init__(self, provider: str = "openai", model: str = "gpt-4o-mini", api_key: str = None):
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.client = None
        
        # LLMクライアントを初期化
        self._initialize_client()
    
    def _initialize_client(self):
        """LLMクライアントを初期化"""
        try:
            if self.provider == "openai":
                import openai
                if self.api_key:
                    openai.api_key = self.api_key
                self.client = openai.OpenAI(api_key=self.api_key)
            elif self.provider == "anthropic":
                import anthropic
                self.client = anthropic.Anthropic(api_key=self.api_key)
            else:
                logger.warning(f"サポートされていないLLMプロバイダー: {self.provider}")
                self.client = None
                
        except ImportError as e:
            logger.warning(f"LLMライブラリがインストールされていません: {e}")
            self.client = None
        except Exception as e:
            logger.error(f"LLMクライアントの初期化に失敗しました: {e}")
            self.client = None
    
    def extract_from_event(self, event: CalendarEvent) -> ExtractedData:
        """イベントからAI抽出を実行"""
        if not self.client:
            logger.warning("LLMクライアントが利用できません")
            return ExtractedData(confidence=0.0)
        
        try:
            # プロンプトを構築
            prompt = self._build_extraction_prompt(event)
            
            # LLMに問い合わせ
            response = self._query_llm(prompt)
            
            # レスポンスを解析
            extracted_data = self._parse_llm_response(response)
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"AI抽出でエラーが発生しました: {e}")
            return ExtractedData(confidence=0.0)
    
    def _build_extraction_prompt(self, event: CalendarEvent) -> str:
        """抽出用のプロンプトを構築"""
        prompt = f"""
以下のGoogleカレンダーイベントから、実在する会社名と人名だけを抽出して、JSONで返してください。
存在しない推測はせず、見つからなければnullにしてください。

**イベント情報:**
- タイトル: {event.title}
- 説明: {event.description or 'なし'}
- 場所: {event.location or 'なし'}
- 出席者: {json.dumps([a.get('displayName', '') for a in event.attendees], ensure_ascii=False) if event.attendees else 'なし'}

**抽出ルール:**
1. 会社名: 株式会社、Inc.などの接尾語があるもの、または明らかに企業名と分かるもの
   - 例: 株式会社サンプル、ABC Inc.、ととのいプラス、サンプル商事
   - カタカナ、ひらがな、漢字の組み合わせでも企業名として認識
   - プラス、サービス、ソリューション、クリニックなどの語尾も企業名の可能性
2. 人名: 日本語の姓名、または明らかに人名と分かるもの
3. 架空の名前や推測は禁止
4. 信頼度スコア（0.0-1.0）を付与

**出力形式:**
```json
{{
  "company_name": "会社名またはnull",
  "person_names": ["人名1", "人名2"]または[],
  "confidence": 0.85
}}
```

**注意:** 必ず有効なJSON形式で返してください。
"""
        return prompt.strip()
    
    def _query_llm(self, prompt: str) -> str:
        """LLMに問い合わせ"""
        try:
            if self.provider == "openai":
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "あなたは会社名と人名を正確に抽出するAIアシスタントです。"},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,  # 低い温度で一貫性を保つ
                    max_tokens=500
                )
                return response.choices[0].message.content
            
            elif self.provider == "anthropic":
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=500,
                    temperature=0.1,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                return response.content[0].text
            
        except Exception as e:
            logger.error(f"LLM問い合わせでエラーが発生しました: {e}")
            raise
        
        return ""
    
    def _parse_llm_response(self, response: str) -> ExtractedData:
        """LLMレスポンスを解析"""
        try:
            # JSON部分を抽出
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # JSONブロックがない場合、レスポンス全体を試行
                json_str = response
            
            # JSONをパース
            data = json.loads(json_str)
            
            return ExtractedData(
                company_name=data.get('company_name'),
                person_names=data.get('person_names', []),
                confidence=data.get('confidence', 0.0)
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"LLMレスポンスのJSON解析に失敗しました: {e}")
            logger.debug(f"レスポンス内容: {response}")
            return ExtractedData(confidence=0.0)
        except Exception as e:
            logger.error(f"LLMレスポンスの解析でエラーが発生しました: {e}")
            return ExtractedData(confidence=0.0)


class HybridExtractor:
    """規則ベース + AI抽出のハイブリッド抽出器"""
    
    def __init__(self, rule_extractor: RuleBasedExtractor, ai_extractor: AIExtractor):
        self.rule_extractor = rule_extractor
        self.ai_extractor = ai_extractor
        self.confidence_threshold = 0.8
    
    def extract_from_event(self, event: CalendarEvent) -> ExtractedData:
        """ハイブリッド抽出を実行"""
        try:
            # 1次：規則ベース抽出
            rule_result = self.rule_extractor.extract_from_event(event.dict())
            
            # 信頼度が閾値を超えている場合は規則ベース結果を返す
            if rule_result.confidence >= self.confidence_threshold:
                logger.debug(f"規則ベース抽出で十分な信頼度を達成: {rule_result.confidence}")
                return rule_result
            
            # 2次：AI抽出
            logger.debug("規則ベース抽出の信頼度が不足、AI抽出を実行")
            ai_result = self.ai_extractor.extract_from_event(event)
            
            # 3次：結果の統合とバリデーション
            final_result = self._merge_and_validate_results(rule_result, ai_result)
            
            return final_result
            
        except Exception as e:
            logger.error(f"ハイブリッド抽出でエラーが発生しました: {e}")
            return ExtractedData(confidence=0.0)
    
    def _merge_and_validate_results(self, rule_result: ExtractedData, 
                                  ai_result: ExtractedData) -> ExtractedData:
        """規則ベースとAI抽出結果を統合・バリデーション"""
        # 会社名の統合
        company_name = self._merge_company_names(
            rule_result.company_name, 
            ai_result.company_name,
            rule_result.confidence,
            ai_result.confidence
        )
        
        # 人名の統合
        person_names = self._merge_person_names(
            rule_result.person_names,
            ai_result.person_names,
            rule_result.confidence,
            ai_result.confidence
        )
        
        # 信頼度の計算
        confidence = self._calculate_merged_confidence(
            rule_result.confidence,
            ai_result.confidence,
            company_name,
            person_names
        )
        
        return ExtractedData(
            company_name=company_name,
            person_names=person_names,
            confidence=confidence
        )
    
    def _merge_company_names(self, rule_company: Optional[str], 
                            ai_company: Optional[str],
                            rule_confidence: float,
                            ai_confidence: float) -> Optional[str]:
        """会社名を統合"""
        if not rule_company and not ai_company:
            return None
        
        if not rule_company:
            return ai_company
        
        if not ai_company:
            return rule_company
        
        # 両方ある場合、信頼度の高い方を選択
        if rule_confidence >= ai_confidence:
            return rule_company
        else:
            return ai_company
    
    def _merge_person_names(self, rule_names: List[str],
                           ai_names: List[str],
                           rule_confidence: float,
                           ai_confidence: float) -> List[str]:
        """人名を統合"""
        if not rule_names and not ai_names:
            return []
        
        if not rule_names:
            return ai_names
        
        if not ai_names:
            return rule_names
        
        # 両方ある場合、重複を除去して統合
        all_names = set(rule_names + ai_names)
        
        # 信頼度の高い方の名前を優先
        if rule_confidence >= ai_confidence:
            # 規則ベースの名前を優先し、AIの名前で補完
            final_names = list(rule_names)
            for ai_name in ai_names:
                if ai_name not in rule_names:
                    final_names.append(ai_name)
            return final_names
        else:
            # AIの名前を優先し、規則ベースの名前で補完
            final_names = list(ai_names)
            for rule_name in rule_names:
                if rule_name not in ai_names:
                    final_names.append(rule_name)
            return final_names
    
    def _calculate_merged_confidence(self, rule_confidence: float,
                                   ai_confidence: float,
                                   company_name: Optional[str],
                                   person_names: List[str]) -> float:
        """統合後の信頼度を計算"""
        # 基本信頼度（重み付き平均）
        base_confidence = (rule_confidence * 0.6 + ai_confidence * 0.4)
        
        # 結果の豊富さによる補正
        if company_name and person_names:
            base_confidence += 0.1
        elif company_name or person_names:
            base_confidence += 0.05
        
        # 信頼度の範囲を制限
        return min(max(base_confidence, 0.0), 1.0)
    
    def set_confidence_threshold(self, threshold: float):
        """信頼度閾値を設定"""
        self.confidence_threshold = max(0.0, min(1.0, threshold))
        logger.info(f"信頼度閾値を設定しました: {self.confidence_threshold}")
