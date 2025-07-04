# src/pipeline/stage3_global_struct/rhythm_analyzer.py
from typing import List, Dict
from src.core.data_models import EventUnit
from src.core.quantification.conflict_scorer import ConflictScorer


class RhythmAnalyzer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conflict_scorer = ConflictScorer(config)

    def detect_turning_points(self, events: List[EventUnit]) -> List[Dict[str, Any]]:
        """检测叙事转折点"""
        turning_points = []

        # 计算每个事件的冲突强度
        intensities = [self.conflict_scorer.calculate_intensity(e) for e in events]

        # 识别强度峰值作为转折点
        for i in range(1, len(intensities) - 1):
            if intensities[i] > intensities[i - 1] and intensities[i] > intensities[i + 1]:
                # 超过阈值的峰值才被认为是转折点
                if intensities[i] >= self.config.get('turning_point_threshold', 7):
                    turning_points.append({
                        "position": f"{i + 1}/{len(events)}",
                        "event_id": events[i].event_id,
                        "intensity": intensities[i]
                    })

        return turning_points

    def calculate_pacing_score(self, events: List[EventUnit]) -> float:
        """计算整体叙事节奏评分"""
        if not events:
            return 0.0

        # 计算平均事件密度（每章节事件数）
        chapter_counts = {}
        for event in events:
            for chap in event.source_chapters:
                chapter_counts[chap] = chapter_counts.get(chap, 0) + 1

        avg_density = sum(chapter_counts.values()) / len(chapter_counts)

        # 计算冲突强度方差
        intensities = [self.conflict_scorer.calculate_intensity(e) for e in events]
        mean_intensity = sum(intensities) / len(intensities)
        variance = sum((i - mean_intensity) ** 2 for i in intensities) / len(intensities)

        # 评分公式（可根据需要调整）
        pacing_score = min(10, avg_density * 0.5 + variance * 0.3)
        return round(pacing_score, 1)