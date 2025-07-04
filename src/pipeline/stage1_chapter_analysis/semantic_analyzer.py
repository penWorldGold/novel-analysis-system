# semantic_analyzer.py
import json
import os
import logging
from typing import Dict, Any
from src.core.ai_adapter import AIAdapter  # 假设的AI适配器接口
from src.core.data_models.chapter_schema import ChapterAnalysisResult  # 数据模型


class SemanticAnalyzer:
    """
    单章多维度语义分析器
    输入：单章TXT文本
    输出：结构化章节分析结果（JSON格式）
    """

    def __init__(self, ai_adapter: AIAdapter, prompt_template: str):
        """
        初始化分析器

        Args:
            ai_adapter: AI适配器实例
            prompt_template: 分析提示词模板
        """
        self.ai_adapter = ai_adapter
        self.prompt_template = prompt_template
        self.logger = logging.getLogger(__name__)
        self.logger.info("SemanticAnalyzer initialized")

    def analyze(self, chapter_id: str, chapter_text: str) -> Dict[str, Any]:
        """
        分析单章内容

        Args:
            chapter_id: 章节ID（如0001）
            chapter_text: 章节文本内容

        Returns:
            分析结果字典
        """
        # 构造完整提示词
        full_prompt = self.prompt_template.format(
            chapter_id=chapter_id,
            chapter_text=chapter_text[:10000]  # 限制长度
        )

        self.logger.info("Analyzing chapter %s (text length: %d)", chapter_id, len(chapter_text))

        # 调用AI分析
        try:
            response = self.ai_adapter.generate(full_prompt)
            self.logger.debug("AI response received for chapter %s", chapter_id)

            # 尝试解析JSON
            try:
                result = json.loads(response)
                # 基本验证
                if "chapter_id" not in result or "core_events" not in result:
                    raise ValueError("Invalid JSON structure")

                return result
            except json.JSONDecodeError:
                # 尝试从非纯JSON响应中提取JSON
                self.logger.warning("AI response not pure JSON, attempting extraction")
                return self._extract_json(response)

        except Exception as e:
            self.logger.error("Analysis failed for chapter %s: %s", chapter_id, str(e))
            # 返回错误结构
            return {
                "chapter_id": chapter_id,
                "error": str(e),
                "core_events": [],
                "character_relations": [],
                "time_environment": {},
                "implicit_info": {},
                "themes": []
            }

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """
        从文本中提取JSON内容

        Args:
            text: 包含JSON的文本

        Returns:
            解析后的JSON字典
        """
        try:
            # 尝试定位JSON开始位置
            start_idx = text.find('{')
            end_idx = text.rfind('}')

            if start_idx == -1 or end_idx == -1:
                raise ValueError("No JSON found in response")

            json_str = text[start_idx:end_idx + 1]
            return json.loads(json_str)
        except Exception as e:
            self.logger.error("JSON extraction failed: %s", str(e))
            raise ValueError(f"JSON extraction failed: {str(e)}")

    def analyze_and_save(self, chapter_path: str, output_dir: str) -> str:
        """
        分析章节文件并保存结果

        Args:
            chapter_path: 章节文件路径
            output_dir: 输出目录

        Returns:
            结果文件路径
        """
        # 从文件名提取章节ID（0001.txt -> 0001）
        chapter_id = os.path.basename(chapter_path).split('.')[0]

        # 读取章节内容
        try:
            with open(chapter_path, 'r', encoding='utf-8') as f:
                chapter_text = f.read()
        except Exception as e:
            self.logger.error("Error reading chapter file %s: %s", chapter_path, str(e))
            raise

        # 执行分析
        result = self.analyze(chapter_id, chapter_text)

        # 保存结果
        output_path = os.path.join(output_dir, f"{chapter_id}.json")
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            self.logger.info("Analysis saved for chapter %s at %s", chapter_id, output_path)
            return output_path
        except Exception as e:
            self.logger.error("Error saving analysis result: %s", str(e))
            raise