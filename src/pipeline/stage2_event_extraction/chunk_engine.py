import os
import json
import logging
import math
from typing import List, Dict, Tuple, Optional
from src.core.data_models.chapter_schema import ChapterAnalysisResult


class ChunkEngine:
    """
    智能迭代分块引擎，用于处理大规模章节分析数据

    功能：
    - 动态调整分块大小，确保事件完整性
    - 智能检测事件边界
    - 迭代处理剩余事件

    构造函数:
    :param initial_chunk_size: int - 初始分块大小（章节数）
    :param min_chunk_size: int - 最小分块大小
    :param max_chunk_size: int - 最大分unk大小
    """

    def __init__(self, initial_chunk_size: int = 50,
                 min_chunk_size: int = 10,
                 max_chunk_size: int = 100):
        self.initial_chunk_size = initial_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.logger = logging.getLogger(__name__)
        self.logger.info("ChunkEngine initialized with chunk size: %d-%d",
                         min_chunk_size, max_chunk_size)

    def load_chapters(self, analysis_dir: str) -> List[ChapterAnalysisResult]:
        """
        加载所有章节分析结果

        :param analysis_dir: str - 分析结果目录
        :return: 章节分析结果列表
        """
        chapter_files = sorted([
            f for f in os.listdir(analysis_dir)
            if f.endswith('.json') and f[0].isdigit()
        ])

        chapters = []
        for file in chapter_files:
            path = os.path.join(analysis_dir, file)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    chapters.append(ChapterAnalysisResult(**data))
            except Exception as e:
                self.logger.error("Error loading chapter %s: %s", file, str(e))

        self.logger.info("Loaded %d chapters from %s", len(chapters), analysis_dir)
        return chapters

    def create_initial_chunks(self, chapters: List[ChapterAnalysisResult]) -> List[List[ChapterAnalysisResult]]:
        """
        创建初始分块

        :param chapters: 所有章节列表
        :return: 分块列表
        """
        num_chunks = math.ceil(len(chapters) / self.initial_chunk_size)
        chunks = [chapters[i:i + self.initial_chunk_size]
                  for i in range(0, len(chapters), self.initial_chunk_size)]

        self.logger.info("Created %d initial chunks of size %d",
                         num_chunks, self.initial_chunk_size)
        return chunks

    def analyze_chunk_completeness(self, chunk: List[ChapterAnalysisResult]) -> Tuple[float, List[str]]:
        """
        分析分块的事件完整性

        :param chunk: 当前分块
        :return: (完整性评分, 未完成事件列表)
        """
        # 简单实现：检查每个章节的最后事件是否完整
        incomplete_events = []
        for chapter in chunk:
            if chapter.core_events:
                last_event = chapter.core_events[-1]
                # 检查事件是否有结果和影响值
                if not last_event.get('outcome') or last_event.get('impact_value') is None:
                    incomplete_events.append(last_event['description'])

        completeness = 1 - (len(incomplete_events) / len(chunk)
                            self.logger.debug("Chunk completeness: %.2f, incomplete events: %d",
                            completeness, len(incomplete_events))
        return completeness, incomplete_events

    def calculate_next_chunk_size(self, current_size: int,
                                  completeness: float,
                                  remaining_chapters: int) -> int:
        """
        计算下一个分块大小

        :param current_size: 当前分块大小
        :param completeness: 当前分块完整性
        :param remaining_chapters: 剩余章节数
        :return: 下一个分块大小
        """
        if completeness < 0.7:  # 完整性低，减小分块
            new_size = max(self.min_chunk_size, int(current_size * 0.8))
        elif completeness > 0.9:  # 完整性高，增大分块
            new_size = min(self.max_chunk_size, int(current_size * 1.2))
        else:  # 保持相似大小
            new_size = current_size

        # 不超过剩余章节数
        new_size = min(new_size, remaining_chapters)
        self.logger.info("Next chunk size: %d (prev: %d, completeness: %.2f)",
                         new_size, current_size, completeness)
        return new_size

    def create_next_chunk(self, incomplete_events: List[str],
                          remaining_chapters: List[ChapterAnalysisResult],
                          chunk_size: int) -> List[ChapterAnalysisResult]:
        """
        创建下一个分块（包含未完成事件和新章节）

        :param incomplete_events: 未完成事件列表
        :param remaining_chapters: 剩余章节
        :param chunk_size: 分块大小
        :return: 新分块
        """
        # 简单实现：取剩余章节的前chunk_size个
        new_chunk = remaining_chapters[:chunk_size]

        # 记录未完成事件数
        self.logger.debug("New chunk created with %d chapters, carrying %d incomplete events",
                          len(new_chunk), len(incomplete_events))
        return new_chunk

    def process_all_chapters(self, analysis_dir: str) -> List[List[ChapterAnalysisResult]]:
        """
        处理所有章节，生成完整的分块序列

        :param analysis_dir: 分析结果目录
        :return: 分块列表
        """
        all_chapters = self.load_chapters(analysis_dir)
        chunks = self.create_initial_chunks(all_chapters)
        final_chunks = []

        current_chunk_size = self.initial_chunk_size
        remaining_chapters = all_chapters

        while remaining_chapters:
            # 处理当前分块
            chunk = remaining_chapters[:current_chunk_size]
            remaining_chapters = remaining_chapters[current_chunk_size:]

            # 分析完整性
            completeness, incomplete_events = self.analyze_chunk_completeness(chunk)
            final_chunks.append(chunk)

            # 如果没有剩余章节，结束
            if not remaining_chapters:
                break

            # 计算下一个分块大小
            current_chunk_size = self.calculate_next_chunk_size(
                current_chunk_size, completeness, len(remaining_chapters)
            )

        self.logger.info("Created %d chunks for %d chapters",
                         len(final_chunks), len(all_chapters))
        return final_chunks