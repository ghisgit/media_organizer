import logging
from pathlib import Path
from typing import Callable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from ..utils.helpers import is_video_file


class MediaFileHandler(FileSystemEventHandler):
    """媒体文件事件处理器"""

    def __init__(self, config, callback: Callable[[Path], None]):
        self.config = config
        self.callback = callback
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        """处理文件创建事件"""
        if not event.is_directory:
            self._process_file(Path(event.src_path))

    def on_moved(self, event):
        """处理文件移动事件"""
        if not event.is_directory:
            self._process_file(Path(event.dest_path))

    def _process_file(self, file_path: Path):
        """处理文件"""
        if is_video_file(file_path):
            self.logger.debug(f"检测到视频文件: {file_path}")
            self.callback(file_path)
        else:
            self.logger.debug(f"跳过非视频文件: {file_path}")


class FileMonitor:
    """文件监控器"""

    def __init__(self, config, callback: Callable[[Path], None]):
        self.config = config
        self.callback = callback
        self.observer = Observer()
        self.handler = MediaFileHandler(config, callback)
        self.logger = logging.getLogger(__name__)

    def start(self):
        """开始监控"""
        for directory in self.config.monitor_directories:
            if directory.exists():
                self.observer.schedule(self.handler, str(directory), recursive=True)
                self.logger.info(f"开始监控目录: {directory}")
            else:
                self.logger.warning(f"监控目录不存在: {directory}")

        self.observer.start()
        self.logger.info("文件监控器已启动")

    def stop(self):
        """停止监控"""
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
            self.logger.info("文件监控器已停止")
        else:
            self.logger.info("文件监控器未运行")
