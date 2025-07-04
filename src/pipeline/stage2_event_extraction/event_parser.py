import json
import logging
from typing import List, Dict, Any
from src.core.ai_adapter import AIAdapter
from src.core.data_models.event_schema import EventUnit


class EventParser:
    """
    事件要素深度提取器

    功能：
    - 使用阶段2提示词调用AI模型
    - 从分块数据中提取事件要素
    - 处理跨章节事件关联

    构造函数:
    :param ai_adapter: AIAdapter - AI适配器实例
    :param prompt_template: str - 事件提取提示词模板
    """

    def __init__(self, ai_adapter: AIAdapter, prompt_template: str):
        self.ai_adapter = ai_adapter
        self.prompt_template = prompt_template
        self.logger = logging.getLogger(__name__)
        self.logger.info("EventParser initialized")

    def prepare_input(self, chunk: List[Dict], chunk_id: int) -> str:
        """
        准备AI输入数据

        :param chunk: 当前分块数据
        :param chunk_id: 分块ID
        :return: 格式化输入文本
        """
        input_text = ""
        for i, chapter in enumerate(chunk):
            # 简化为章节ID和核心事件
            events = "\n".join(
                f"- {e['description']} (置信度: {e['confidence']})"
                for e in chapter.core_events
            )
            input_text += f"章节 {chapter.chapter_id}:\n{events}\n\n"

        self.logger.debug("Prepared input for chunk %d: %d chapters",
                          chunk_id, len(chunk))
        return input_text

    def parse_response(self, response: str, chunk: List[Dict]) -> List[EventUnit]:
        """
        解析AI响应为事件单元

        :param response: AI响应文本
        :param chunk: 原始分块数据
        :return: 事件单元列表
        """
        try:
            # 尝试直接解析JSON
            events = json.loads(response)

            # 转换为事件对象
            event_units = []
            for event in events:
                # 添加来源章节信息
                if 'source_chapters' not in event:
                    event['source_chapters'] = self.find_source_chapters(event, chunk)

                event_units.append(EventUnit(**event))

            self.logger.info("Parsed %d events from chunk", len(event_units))
            return event_units
        except json.JSONDecodeError:
            # 尝试从文本提取JSON
            try:
                start = response.find('{')
                end = response.rfind('}')
                if start != -1 and end != -1:
                    return self.parse_response(response[start:end + 1], chunk)
                else:
                    self.logger.error("No valid JSON found in response")
                    return []
            except Exception as e:
                self.logger.error("Error parsing response: %s", str(e))
                return []

    def find_source_chapters(self, event: Dict, chunk: List[Dict]) -> List[str]:
        """
        查找事件的来源章节

        :param event: 事件数据
        :param chunk: 分块数据
        :return: 章节ID列表
        """
        source_chapters = []
        event_desc = event.get('description', '').lower()

        for chapter in chunk:
            # 检查事件描述是否出现在章节事件中
            for chap_event in chapter.core_events:
                if event_desc in chap_event['description'].lower():
                    source_chapters.append(chapter.chapter_id)
                    break

        return source_chapters

    def extract_events(self, chunk: List[Dict], chunk_id: int) -> List[EventUnit]:
        """
        从分块中提取事件

        :param chunk: 分块数据
        :param chunk_id: 分块ID
        :return: 事件单元列表
        """
        # 准备输入
        input_text = self.prepare_input(chunk, chunk_id)

        # 构造提示词
        prompt = self.prompt_template.format(
            chunk_id=chunk_id,
            num_chapters=len(chunk),
            chapter_content=input_text
        )

        # 调用AI
        try:
            self.logger.info("Extracting events from chunk %d (%d chapters)",
                             chunk_id, len(chunk))
            response = self.ai_adapter.generate(prompt)
            self.logger.debug("AI response received for chunk %d", chunk_id)

            # 解析响应
            return self.parse_response(response, chunk)
        except Exception as e:
            self.logger.error("Event extraction failed for chunk %d: %s",
                              chunk_id, str(e))
            return []