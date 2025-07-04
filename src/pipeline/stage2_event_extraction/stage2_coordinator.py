import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from .chunk_engine import ChunkEngine
from .event_parser import EventParser
from src.core.ai_adapter import AIAdapterFactory
from src.core.utils.prompt_manager import load_prompt
from src.core.data_models.event_schema import EventUnit


class Stage2Coordinator:
    """
    阶段2主控制器

    功能：
    - 协调事件提取流程
    - 管理分块和解析过程
    - 处理跨分块事件关联
    - 保存事件知识库

    构造函数:
    :param config: Dict - 阶段配置字典
    """

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化分块引擎
        self.chunk_engine = ChunkEngine(
            initial_chunk_size=config.get('initial_chunk_size', 50),
            min_chunk_size=config.get('min_chunk_size', 10),
            max_chunk_size=config.get('max_chunk_size', 100)
        )

        # 初始化AI适配器
        ai_config = config.get('ai_config', {})
        self.ai_adapter = AIAdapterFactory.create_adapter(
            ai_type=ai_config.get('type', 'wenxin'),
            api_key=ai_config.get('api_key'),
            model=ai_config.get('model', 'ernie-bot')
        )

        # 加载提示词
        self.prompt_template = load_prompt('stage2')
        if not self.prompt_template:
            self.logger.error("Failed to load prompt for stage2")
            raise ValueError("Stage2 prompt not available")

        # 初始化事件解析器
        self.event_parser = EventParser(
            ai_adapter=self.ai_adapter,
            prompt_template=self.prompt_template
        )

        self.logger.info("Stage2Coordinator initialized")

    def process_stage1_output(self, stage1_output: Dict) -> List[EventUnit]:
        """
        处理阶段1输出，提取所有事件

        :param stage1_output: 阶段1输出结果
        :return: 事件单元列表
        """
        analysis_dir = stage1_output.get('analysis_dir')
        if not analysis_dir or not os.path.exists(analysis_dir):
            self.logger.error("Invalid stage1 output directory: %s", analysis_dir)
            raise ValueError("Missing or invalid stage1 output directory")

        # 创建分块
        chunks = self.chunk_engine.process_all_chapters(analysis_dir)
        all_events = []

        # 处理每个分块
        for i, chunk in enumerate(chunks):
            self.logger.info("Processing chunk %d/%d (%d chapters)",
                             i + 1, len(chunks), len(chunk))

            events = self.event_parser.extract_events(chunk, i)
            all_events.extend(events)

            # 进度日志
            if (i + 1) % 5 == 0 or (i + 1) == len(chunks):
                self.logger.info("Processed %d chunks, total events: %d",
                                 i + 1, len(all_events))

        # 后处理：事件去重和合并
        processed_events = self.post_process_events(all_events)
        return processed_events

    def post_process_events(self, events: List[EventUnit]) -> List[EventUnit]:
        """
        后处理事件：去重和合并

        :param events: 原始事件列表
        :return: 处理后事件列表
        """
        # 简单实现：基于事件描述去重
        unique_events = {}
        for event in events:
            key = event.description.lower()
            if key in unique_events:
                # 合并来源章节
                unique_events[key].source_chapters = list(
                    set(unique_events[key].source_chapters + event.source_chapters)
                else:
                unique_events[key] = event

                self.logger.info("Post-processed events: %d -> %d",
                                 len(events), len(unique_events))
        return list(unique_events.values())

    def save_events(self, events: List[EventUnit], output_path: str):
        """
        保存事件知识库

        :param events: 事件列表
        :param output_path: 输出路径
        """
        # 转换为字典列表
        event_dicts = [event.dict() for event in events]

        # 保存为Parquet
        parquet_path = os.path.join(output_path, "events.parquet")
        df = pd.DataFrame(event_dicts)
        df.to_parquet(parquet_path, index=False)

        # 同时保存为JSONL
        jsonl_path = os.path.join(output_path, "events.jsonl")
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for event in event_dicts:
                f.write(json.dumps(event, ensure_ascii=False) + '\n')

        self.logger.info("Saved %d events to %s and %s",
                         len(events), parquet_path, jsonl_path)

    def run(self, stage1_output: Dict) -> Dict[str, Any]:
        """
        运行阶段2处理流程

        :param stage1_output: 阶段1输出结果
        :return: 处理结果统计
        """
        # 准备输出目录
        output_dir = self.config.get('output_dir', 'data/stage2_output')
        os.makedirs(output_dir, exist_ok=True)

        # 处理事件
        start_time = datetime.now()
        events = self.process_stage1_output(stage1_output)

        # 保存结果
        self.save_events(events, output_dir)

        # 计算处理时间
        duration = datetime.now() - start_time

        return {
            'status': 'completed',
            'event_count': len(events),
            'output_dir': output_dir,
            'processing_time': str(duration),
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat()
        }