# src/pipeline/stage3_global_struct/world_modeler.py
from typing import List, Dict
from src.core.data_models import EventUnit


class WorldModeler:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def extract_core_rules(self, events: List[EventUnit]) -> List[str]:
        """提取世界观核心规则体系"""
        rules = set()

        # 从事件结果中提取规则
        for event in events:
            if "规则" in event.core_elements.outcome:
                rules.add(event.core_elements.outcome)

        return list(rules)[:self.config.get('max_rules', 5)]

    def build_space_structure(self, events: List[EventUnit]) -> List[str]:
        """构建空间结构描述"""
        locations = {}

        # 统计地点出现频率
        for event in events:
            loc = event.temporal.get('location', '未知')
            locations[loc] = locations.get(loc, 0) + 1

        # 按频率排序并返回描述
        sorted_locs = sorted(locations.items(), key=lambda x: x[1], reverse=True)
        return [f"{loc}（出现次数: {count}）" for loc, count in sorted_locs[:5]]

    def track_theme_evolution(self, events: List[EventUnit]) -> List[Dict[str, Any]]:
        """追踪主题演化轨迹"""
        if not events:
            return []

        # 将事件分成4个阶段
        num_events = len(events)
        phase_size = num_events // 4
        phases = []

        for i in range(4):
            start_idx = i * phase_size
            end_idx = (i + 1) * phase_size if i < 3 else num_events
            phase_events = events[start_idx:end_idx]

            # 统计阶段内主题
            theme_counter = {}
            for event in phase_events:
                for theme in event.themes:
                    theme_counter[theme] = theme_counter.get(theme, 0) + 1

            # 获取主导主题
            sorted_themes = sorted(theme_counter.items(), key=lambda x: x[1], reverse=True)
            dominant_themes = [theme for theme, count in sorted_themes[:3]]

            phases.append({
                "phase": f"阶段{i + 1}",
                "dominant_themes": dominant_themes
            })

        return phases