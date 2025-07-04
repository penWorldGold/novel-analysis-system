# stage1_coordinator.py
import os
import logging
from typing import Dict
from .chapter_segmenter import ChapterSegmenter
from .semantic_analyzer import SemanticAnalyzer
from src.core.ai_adapter import AIAdapterFactory  # 假设的AI适配器工厂
from src.core.utils.prompt_manager import load_prompt  # 提示词管理


class Stage1Coordinator:
    """
    阶段1主控制器
    协调章节分割和单章分析流程
    """

    def __init__(self, config: Dict):
        """
        初始化协调器

        Args:
            config: 阶段配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化组件
        self.segmenter = ChapterSegmenter(
            custom_patterns=config.get('custom_chapter_patterns', [])
        )

        # 创建AI适配器
        ai_config = config.get('ai_config', {})
        self.ai_adapter = AIAdapterFactory.create_adapter(
            ai_type=ai_config.get('type', 'wenxin'),
            api_key=ai_config.get('api_key'),
            model=ai_config.get('model', 'ernie-bot')
        )

        # 加载提示词
        self.prompt_template = load_prompt('stage1')
        if not self.prompt_template:
            self.logger.error("Failed to load prompt for stage1")
            raise ValueError("Stage1 prompt not available")

        self.analyzer = SemanticAnalyzer(
            ai_adapter=self.ai_adapter,
            prompt_template=self.prompt_template
        )

        self.logger.info("Stage1Coordinator initialized")

    def process_novel(self, novel_path: str) -> Dict[str, Any]:
        """
        处理整本小说

        Args:
            novel_path: 小说文件路径

        Returns:
            处理结果统计
        """
        # 准备输出目录
        original_dir = self.config['output']['original_chapters']
        analysis_dir = self.config['output']['analysis_results']

        os.makedirs(original_dir, exist_ok=True)
        os.makedirs(analysis_dir, exist_ok=True)

        self.logger.info("Starting novel processing: %s", novel_path)

        # 步骤1: 章节分割
        try:
            num_chapters = self.segmenter.segment_to_files(
                novel_path,
                original_dir,
                encoding=self.config.get('encoding', 'utf-8')
            )
            self.logger.info("Segmented into %d chapters", num_chapters)
        except Exception as e:
            self.logger.error("Chapter segmentation failed: %s", str(e))
            return {
                'status': 'error',
                'message': f'Segmentation failed: {str(e)}',
                'chapters_processed': 0
            }

        # 步骤2: 逐章分析
        processed = 0
        failed_chapters = []

        # 获取所有章节文件
        chapter_files = sorted([
            f for f in os.listdir(original_dir)
            if f.endswith('.txt') and f[0].isdigit()
        ])

        self.logger.info("Starting analysis of %d chapters", len(chapter_files))

        for chapter_file in chapter_files:
            chapter_path = os.path.join(original_dir, chapter_file)
            chapter_id = chapter_file.split('.')[0]

            try:
                # 分析并保存结果
                self.analyzer.analyze_and_save(chapter_path, analysis_dir)
                processed += 1

                # 进度报告
                if processed % 10 == 0:
                    self.logger.info("Processed %d/%d chapters", processed, len(chapter_files))

            except Exception as e:
                self.logger.error("Failed to process chapter %s: %s", chapter_id, str(e))
                failed_chapters.append(chapter_id)

        # 返回处理结果
        result = {
            'status': 'completed' if not failed_chapters else 'partial',
            'total_chapters': len(chapter_files),
            'processed': processed,
            'failed': len(failed_chapters),
            'failed_chapters': failed_chapters,
            'original_dir': original_dir,
            'analysis_dir': analysis_dir
        }

        self.logger.info("Stage1 processing completed. Result: %s", result)
        return result