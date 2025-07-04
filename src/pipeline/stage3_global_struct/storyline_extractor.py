# src/pipeline/stage3_global_struct/storyline_extractor.py
import json
from typing import List, Dict, Any
from src.core.data_models import EventUnit, GlobalStructure
from src.core.ai_adapter import AIAdapter
from src.core.utils.prompt_manager import load_prompt_template


class StorylineExtractor:
    def __init__(self, ai_adapter: AIAdapter, config: Dict[str, Any]):
        self.ai_adapter = ai_adapter
        self.config = config
        self.prompt_template = load_prompt_template("stage3_storyline")

    def extract_main_storyline(self, events: List[EventUnit]) -> List[Dict[str, Any]]:
        """提取主线故事的关键事件序列"""
        # 过滤高影响值的事件作为主线候选
        min_impact = self.config.get('main_story_min_impact', 7)
        candidate_events = [e for e in events if e.core_elements.impact_value >= min_impact]

        # 按时间顺序排序
        candidate_events.sort(key=lambda e: e.temporal.position)

        # 构建AI输入
        input_data = {
            "events": [e.model_dump() for e in candidate_events],
            "min_impact": min_impact,
            "max_events": self.config.get('main_story_max_events', 20)
        }

        # 调用AI分析主线
        response = self.ai_adapter.call_ai(
            prompt=self.prompt_template,
            input_data=json.dumps(input_data, ensure_ascii=False)
        )

        try:
            result = json.loads(response)
            return result.get("main_storyline", [])
        except json.JSONDecodeError:
            # 错误处理：尝试手动提取关键事件
            return self.fallback_main_story(candidate_events)

    def fallback_main_story(self, events: List[EventUnit]) -> List[Dict[str, Any]]:
        """AI解析失败时的后备主线提取逻辑"""
        return [{
            "event_id": e.event_id,
            "type": e.core_elements.conflict,
            "description": e.description
        } for e in events[:min(15, len(events))]]

    def identify_subplots(self, events: List[EventUnit]) -> List[Dict[str, Any]]:
        """识别所有重要支线"""
        # 按人物分组事件
        character_events = {}
        for event in events:
            for char in event.participants.core_characters:
                if char not in character_events:
                    character_events[char] = []
                character_events[char].append(event)

        # 筛选包含多个事件的支线
        subplots = []
        for char, events in character_events.items():
            if len(events) > 2:  # 至少3个事件才能构成支线
                # 计算与主线的耦合度（简化版）
                main_chars = self.config.get('main_characters', [])
                coupling_score = 0.8 if char in main_chars else 0.3

                subplots.append({
                    "name": f"{char}的支线",
                    "core_events": [e.event_id for e in events],
                    "coupling_score": coupling_score
                })

        return subplots