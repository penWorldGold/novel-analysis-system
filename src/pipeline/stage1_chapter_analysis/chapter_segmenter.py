# chapter_segmenter.py
import re
import os
import logging
from typing import List, Tuple


class ChapterSegmenter:
    """
    自适应章节分割器，处理标准和非标准章节结构
    输入：百万字级别的小说TXT文本
    输出：分割后的单章TXT文件
    """
    DEFAULT_PATTERNS = [
        r'第[零一二三四五六七八九十百千万\d]+章\s+.*',  # 第一章 标题
        r'第[零一二三四五六七八九十百千万\d]+节\s+.*',  # 第一节 标题
        r'[Cc]hapter\s*[\dIVX]+[\.\s].*',  # Chapter 1. Title
        r'卷[之零一二三四五六七八九十百千万\d]+：.*',  # 卷之一：标题
        r'【第[零一二三四五六七八九十百千万\d]+章】.*'  # 【第一章】标题
    ]

    def __init__(self, custom_patterns: List[str] = None):
        """
        初始化章节分割器

        Args:
            custom_patterns: 用户自定义的章节标题正则模式
        """
        self.patterns = self.DEFAULT_PATTERNS
        if custom_patterns:
            self.patterns += custom_patterns

        self.logger = logging.getLogger(__name__)
        self.logger.info("ChapterSegmenter initialized with %d patterns", len(self.patterns))

    def segment(self, text: str) -> List[Tuple[int, str, str]]:
        """
        执行章节分割

        Args:
            text: 完整的小说文本内容

        Returns:
            章节列表: (章节序号, 章节标题, 章节内容)
        """
        if not text.strip():
            self.logger.warning("Empty text provided for segmentation")
            return []

        # 编译正则表达式
        compiled_patterns = [re.compile(p, re.IGNORECASE) for p in self.patterns]

        # 查找所有章节标题位置
        matches = []
        for pattern in compiled_patterns:
            for match in pattern.finditer(text):
                matches.append((match.start(), match.group(0)))

        # 按位置排序
        matches.sort(key=lambda x: x[0])

        # 如果没有找到章节标题
        if not matches:
            self.logger.info("No chapter headers found. Treating as single chapter.")
            return [(1, "全文", text)]

        # 提取章节
        chapters = []
        start_index = 0
        chapter_count = 0

        for i, (pos, title) in enumerate(matches):
            # 跳过第一个匹配点之前的内容（通常是前言）
            if i == 0 and pos > 0:
                self.logger.debug("Skipping preface of length %d", pos)
                start_index = pos
                continue

            # 当前章节内容结束位置
            end_index = matches[i + 1][0] if i + 1 < len(matches) else len(text)

            # 提取章节内容
            chapter_content = text[start_index:end_index].strip()
            chapter_count += 1

            # 添加到结果列表
            chapters.append((chapter_count, title.strip(), chapter_content))
            self.logger.debug("Chapter %d detected: %s (length: %d)",
                              chapter_count, title, len(chapter_content))

            # 更新起始位置
            start_index = end_index

        self.logger.info("Segmented %d chapters from text", chapter_count)
        return chapters

    def segment_to_files(self, file_path: str, output_dir: str, encoding='utf-8') -> int:
        """
        从文件读取文本并分割章节到指定目录

        Args:
            file_path: 输入文件路径
            output_dir: 输出目录
            encoding: 文件编码

        Returns:
            分割的章节数量
        """
        # 读取文件内容
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                text = f.read()
        except Exception as e:
            self.logger.error("Error reading file %s: %s", file_path, str(e))
            raise

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 执行分割
        chapters = self.segment(text)

        # 写入章节文件
        for idx, title, content in chapters:
            filename = f"{idx:04d}.txt"  # 0001.txt 格式
            output_path = os.path.join(output_dir, filename)

            try:
                with open(output_path, 'w', encoding=encoding) as f:
                    f.write(f"# {title}\n\n")
                    f.write(content)
                self.logger.debug("Chapter %d saved to %s", idx, output_path)
            except Exception as e:
                self.logger.error("Error saving chapter %d: %s", idx, str(e))
                continue

        return len(chapters)