# src/scanners/file_scanner.py (修改版本)
import logging
from pathlib import Path
from typing import Iterator, Tuple, List
from ..utils.helpers import is_video_file, format_file_size


class FileScanner:
    """文件扫描器 - 调整检查顺序"""

    def __init__(self, processed_files_db, config):
        self.processed_files_db = processed_files_db
        self.config = config
        self.logger = logging.getLogger(__name__)

    def scan_directory(
        self, directory: Path, check_size: bool = True
    ) -> Iterator[Tuple[Path, int]]:
        """扫描目录中的视频文件"""
        if not directory.exists():
            self.logger.warning(f"目录不存在: {directory}")
            return

        self.logger.info(f"开始扫描目录: {directory}")

        stats = {"total": 0, "video": 0, "skipped": 0, "found": 0}

        try:
            for file_path in directory.rglob("*"):
                if not file_path.is_file():
                    continue

                stats["total"] += 1

                should_skip, skip_reason = self._should_skip_file(file_path, check_size)
                if should_skip:
                    stats["skipped"] += 1
                    self.logger.debug(f"跳过文件: {file_path} - {skip_reason}")
                    continue

                stats["video"] += 1
                stats["found"] += 1

                try:
                    file_size = file_path.stat().st_size
                    self.logger.debug(
                        f"找到文件: {file_path} ({format_file_size(file_size)})"
                    )
                    yield file_path, file_size
                except (OSError, PermissionError) as e:
                    stats["skipped"] += 1
                    self.logger.warning(f"无法访问文件: {file_path} - {e}")

        except Exception as e:
            self.logger.error(f"扫描目录时发生错误 {directory}: {e}")

        self.logger.info(
            f"扫描完成: {directory} - "
            f"总文件: {stats['total']}, "
            f"视频文件: {stats['video']}, "
            f"找到: {stats['found']}, "
            f"跳过: {stats['skipped']}"
        )

    def quick_scan_directories(
        self, directories: List[Path], check_size: bool = True
    ) -> Iterator[Tuple[Path, int]]:
        """快速扫描多个目录"""
        self.logger.info(f"开始快速扫描 {len(directories)} 个目录")

        total_files = 0
        for directory in directories:
            if not directory.exists():
                self.logger.warning(f"目录不存在，跳过: {directory}")
                continue

            try:
                for file_path, file_size in self.scan_directory(directory, check_size):
                    total_files += 1
                    yield file_path, file_size
            except Exception as e:
                self.logger.error(f"扫描目录失败 {directory}: {e}")
                continue

        self.logger.info(f"快速扫描完成，共处理 {total_files} 个文件")

    def _should_skip_file(self, file_path: Path, check_size: bool) -> Tuple[bool, str]:
        """检查是否应该跳过文件 - 调整检查顺序"""
        # 1. 首先检查是否是视频文件
        if not is_video_file(file_path):
            return True, "不是视频文件"

        # 2. 检查忽略模式
        filename = file_path.name.lower()
        for pattern in self.config.ignore_patterns:
            if pattern.startswith("*") and filename.endswith(pattern[1:]):
                return True, f"匹配忽略模式: {pattern}"
            elif pattern == filename:
                return True, f"匹配忽略模式: {pattern}"

        # 3. 文件大小检查（可选）- 注意：这里不进行稳定性检查
        # 稳定性检查将在后续流程中进行
        if check_size:
            try:
                file_size = file_path.stat().st_size
                if file_size < self.config.ignore_file_size:
                    formatted_size = format_file_size(file_size)
                    return True, f"文件太小: {formatted_size}"
            except (OSError, PermissionError) as e:
                return True, f"检查文件大小失败: {e}"

        return False, ""
