# src/pipeline/stage3_global_struct/stage3_coordinator.py
import json
import time
from typing import Dict, List
from src.core.data_models import EventUnit, GlobalStructure
from .storyline_extractor import StorylineExtractor
from .rhythm_analyzer import RhythmAnalyzer
from .world_modeler import WorldModeler


class Stage3Coordinator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storyline_extractor = StorylineExtractor(config['ai_adapter'], config)
        self.rhythm_analyzer = RhythmAnalyzer(config)
        self.world_modeler = WorldModeler(config)

    def load_events(self, event_kb_path: str) -> List[EventUnit]:
        """加载事件知识库"""
        # 实际实现中应从Parquet文件加载
        # 这里简化为从JSON文件加载
        with open(event_kb_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return [EventUnit(**item) for item in data]

    def reconstruct_global_structure(self, events: List[EventUnit]) -> GlobalStructure:
        """重构全局故事结构"""
        # 提取主线故事
        main_storyline = self.storyline_extractor.extract_main_storyline(events)

        # 识别支线
        subplots = self.storyline_extractor.identify_subplots(events)

        # 分析叙事节奏
        turning_points = self.rhythm_analyzer.detect_turning_points(events)
        pacing_score = self.rhythm_analyzer.calculate_pacing_score(events)

        # 构建世界观模型
        core_rules = self.world_modeler.extract_core_rules(events)
        space_structure = self.world_modeler.build_space_structure(events)
        theme_evolution = self.world_modeler.track_theme_evolution(events)

        # 构建全局结构对象
        return GlobalStructure(
            main_storyline=main_storyline,
            subplots=subplots,
            rhythm_analysis={
                "turning_points": turning_points,
                "pacing_score": pacing_score
            },
            world_model={
                "core_rules": core_rules,
                "space_structure": space_structure
            },
            theme_evolution=theme_evolution
        )

    def save_global_structure(self, structure: GlobalStructure, output_path: str):
        """保存全局结构报告"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(structure.model_dump(), f, ensure_ascii=False, indent=2)

    def run(self, stage2_output: Dict[str, Any]) -> Dict[str, Any]:
        """运行阶段3处理流程"""
        start_time = time.time()
        result = {
            "status": "completed",
            "error": None,
            "processing_time": 0,
            "output_path": ""
        }

        try:
            # 1. 加载事件知识库
            events = self.load_events(stage2_output['event_kb_path'])

            # 2. 重构全局结构
            global_struct = self.reconstruct_global_structure(events)

            # 3. 保存结果
            output_path = f"{self.config['output_dir']}/global_struct.json"
            self.save_global_structure(global_struct, output_path)

            # 4. 生成统计信息
            result.update({
                "main_events": len(global_struct.main_storyline),
                "subplots": len(global_struct.subplots),
                "turning_points": len(global_struct.rhythm_analysis['turning_points']),
                "pacing_score": global_struct.rhythm_analysis['pacing_score'],
                "output_path": output_path
            })

        except Exception as e:
            result.update({
                "status": "error",
                "error": str(e)
            })

        result["processing_time"] = round(time.time() - start_time, 2)
        return result