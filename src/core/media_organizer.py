import threading
import time
from queue import Queue, Empty
from typing import Dict, Any, Optional, List
from pathlib import Path
import signal
import sys
import traceback

from ..scanners.file_scanner import FileScanner
from ..scanners.file_monitor import FileMonitor
from ..processors.ai_processor import AIProcessor
from ..processors.tmdb_client import TMDBClient
from ..linkers.file_linker import FileLinker
from .database import TMDBCacheDB, ProcessedFilesDB
from ..utils.logging_config import setup_advanced_logging, get_logger
from ..utils.helpers import is_video_file, calculate_md5, format_file_size
from ..utils.error_handlers import CircuitBreaker, retry_with_backoff, ResourceManager
from .health_monitor import (
    HealthMonitor,
    DatabaseHealthCheck,
    FilesystemHealthCheck,
    SystemResourcesHealthCheck,
    APIHealthCheck,
)
from .config import Config, ConfigValidationError

logger = get_logger(__name__)


class MediaOrganizer:
    """
    媒体文件整理器主类
    负责协调文件扫描、监控、处理和资源管理
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logger

        # 资源管理器
        self.resource_manager = ResourceManager()

        # 初始化组件
        self._init_components()

        # 初始化工作队列
        self.raw_file_queue = Queue()
        self.stable_file_queue = Queue()
        self.md5_queue = Queue()

        # 统计信息
        self.stats = {
            "total_files": 0,
            "duplicate_files": 0,
            "stable_files": 0,
            "unstable_files": 0,
            "processed_files": 0,
            "successful_links": 0,
            "failed_files": 0,
            "initial_scan_completed": False,
            "md5_calculated": 0,
            "md5_failed": 0,
            "start_time": time.time(),
        }

        # 控制标志
        self.running = False
        self.initial_scan_in_progress = False

        # 线程管理
        self.workers: List[threading.Thread] = []
        self.initial_scan_thread: Optional[threading.Thread] = None

        # 文件去重集合
        self.pending_files: Dict[str, float] = {}  # 文件路径 -> 添加时间
        self.pending_files_lock = threading.RLock()

        # 性能监控
        self.performance_stats = {
            "files_processed_per_minute": 0,
            "average_processing_time": 0,
            "processing_times": [],
            "last_performance_update": time.time(),
            "last_file_count": 0,
        }

        # 熔断器
        self.ai_circuit_breaker = CircuitBreaker(
            "AIProcessor", failure_threshold=3, reset_timeout=300
        )
        self.tmdb_circuit_breaker = CircuitBreaker(
            "TMDBClient", failure_threshold=5, reset_timeout=300
        )

        # 健康监控
        self.health_monitor = HealthMonitor(check_interval=300)

        # 注册资源清理
        self.resource_manager.register(self, lambda x: x.stop())
        self.resource_manager.register(self.health_monitor, lambda x: x.stop())

        self.logger.info("媒体文件整理器初始化完成")

    def _update_dynamic_config(self):
        """更新可以动态修改的配置"""
        try:
            # 更新日志级别
            setup_advanced_logging(
                level=self.config.log_level,
                log_file="logs/media_organizer.log",
                max_log_size=50,
                backup_count=10,
            )
            self.logger.info(f"日志级别已更新为: {self.config.log_level}")

            # 更新文件链接方法
            if hasattr(self, "file_linker"):
                self.file_linker.link_method = self.config.link_method
                self.logger.info(f"文件链接方法已更新为: {self.config.link_method}")

            # 更新MD5检查设置
            self.logger.info(f"MD5检查设置已更新为: {self.config.use_md5}")

        except Exception as e:
            self.logger.error(f"更新动态配置失败: {e}")

    def _init_components(self) -> None:
        """初始化各个组件 - 修复线程安全问题"""
        try:
            # 在主线程中预先初始化数据库连接
            # 这确保数据库文件和相关设置在主线程中完成
            self.logger.info("初始化数据库连接...")

            # 初始化数据库（在主线程中创建文件和相关表）
            self.tmdb_cache_db = TMDBCacheDB(
                self.config.tmdb_cache_db, self.config.cache_expire_days
            )
            self.processed_files_db = ProcessedFilesDB(self.config.processed_files_db)

            # 预先执行一些简单查询来建立连接
            self.tmdb_cache_db.get_cache_stats()
            self.processed_files_db.get_processed_count()

            self.logger.info("数据库连接初始化完成")

            # 初始化其他组件
            self.file_scanner = FileScanner(self.processed_files_db, self.config)
            self.ai_processor = AIProcessor(self.config)
            self.tmdb_client = TMDBClient(
                self.config.tmdb_api_key, self.tmdb_cache_db, self.config.tmdb_proxy
            )
            self.file_linker = FileLinker(
                self.config.library_path, self.config.anime_directory
            )
            self.file_monitor = FileMonitor(self.config, self._on_new_file_detected)

            # 注册数据库资源
            self.resource_manager.register(
                self.tmdb_cache_db, lambda x: x.connection_pool.close_all()
            )
            self.resource_manager.register(
                self.processed_files_db, lambda x: x.connection_pool.close_all()
            )

        except Exception as e:
            self.logger.error(f"初始化组件失败: {e}")
            self.logger.debug(f"详细错误: {traceback.format_exc()}")
            raise

    def _setup_health_checks(self) -> None:
        """设置健康检查"""
        try:
            self.health_monitor.add_health_check(
                "database", DatabaseHealthCheck(self.processed_files_db)
            )
            self.health_monitor.add_health_check(
                "filesystem",
                FilesystemHealthCheck(
                    self.config.monitor_directories, self.config.library_path
                ),
            )
            self.health_monitor.add_health_check(
                "system_resources", SystemResourcesHealthCheck()
            )
            self.health_monitor.add_health_check(
                "apis", APIHealthCheck(self.tmdb_client, self.ai_processor, self.config)
            )

            self.health_monitor.start()
            self.logger.info("健康监控器已启动")
        except Exception as e:
            self.logger.error(f"设置健康检查失败: {e}")
            self.logger.debug(f"详细错误: {traceback.format_exc()}")

    def _on_new_file_detected(self, file_path: Path) -> None:
        """
        当监控器检测到新文件时的回调 - 调整处理顺序
        """
        if not self.running:
            return

        priority = "low" if self.initial_scan_in_progress else "normal"

        self.logger.info(f"检测到新文件 [{priority}]: {file_path}")

        # 快速基本检查
        if not self._quick_file_check(file_path):
            return

        # 使用绝对路径
        try:
            file_path_str = str(file_path.resolve())
        except Exception as e:
            self.logger.warning(f"无法解析文件路径 {file_path}: {e}")
            return

        # 1. 首先检查是否已经在处理中
        if not self._add_to_pending(file_path_str):
            self.logger.debug(f"文件已在处理队列中，跳过: {file_path}")
            self._update_stats("duplicate_files")
            return

        # 2. 检查是否已处理（在稳定性检查之前）
        if self._is_file_already_processed(file_path):
            self.logger.debug(f"文件已处理，跳过: {file_path}")
            self._remove_from_pending(file_path_str)
            self._update_stats("processed_files")
            return

        # 3. 将文件信息放入原始文件队列进行稳定性检查
        try:
            file_info = {
                "file_path": file_path_str,
                "file_size": file_path.stat().st_size,
                "priority": priority,
                "source": "monitor",
                "detected_time": time.time(),
            }

            self.raw_file_queue.put(file_info)
            self.stats["total_files"] += 1
            self.logger.info(f"新文件加入原始文件队列: {file_path}")
        except Exception as e:
            self.logger.error(f"处理新文件失败 {file_path}: {e}")
            self._remove_from_pending(file_path_str)

    def _is_file_already_processed(self, file_path: Path) -> bool:
        """
        检查文件是否已处理 - 在稳定性检查之前调用
        """
        try:
            file_path_str = str(file_path.resolve())

            # 对于新检测的文件，先不计算MD5（因为文件可能还不稳定）
            # 只检查文件路径是否已处理
            if self.processed_files_db.is_processed(
                file_path_str, md5=None, use_md5=False
            ):
                self.logger.debug(f"文件路径已处理: {file_path}")
                return True

            return False

        except Exception as e:
            self.logger.warning(f"检查文件是否已处理失败 {file_path}: {e}")
            return False

    def _quick_file_check(self, file_path: Path) -> bool:
        """
        快速检查文件基本条件（不检查稳定性）
        """
        try:
            if not file_path.exists():
                self.logger.debug(f"文件不存在: {file_path}")
                return False

            if not file_path.is_file():
                self.logger.debug(f"不是文件: {file_path}")
                return False

            # 检查是否是视频文件
            if not is_video_file(file_path):
                self.logger.debug(f"不是视频文件: {file_path}")
                return False

            # 检查忽略模式
            filename = file_path.name.lower()
            for pattern in self.config.ignore_patterns:
                if pattern.startswith("*") and filename.endswith(pattern[1:]):
                    self.logger.debug(
                        f"跳过文件（匹配忽略模式 {pattern}）: {file_path}"
                    )
                    return False
                elif pattern == filename:
                    self.logger.debug(
                        f"跳过文件（匹配忽略模式 {pattern}）: {file_path}"
                    )
                    return False

            # 注意：这里不检查文件大小，因为文件可能正在移动/下载中
            # 文件大小检查将在稳定性检查之后进行

            return True
        except Exception as e:
            self.logger.debug(f"快速文件检查失败 {file_path}: {e}")
            return False

    def _add_to_pending(self, file_path_str: str) -> bool:
        """
        添加文件到待处理集合，带有容量限制

        Args:
            file_path_str: 文件路径字符串

        Returns:
            是否成功添加
        """
        with self.pending_files_lock:
            # 清理过期条目（超过2小时）
            current_time = time.time()
            expired_files = [
                path
                for path, add_time in self.pending_files.items()
                if current_time - add_time > 7200  # 2小时
            ]
            for expired_file in expired_files:
                del self.pending_files[expired_file]
                self.logger.debug(f"清理过期待处理文件: {expired_file}")

            # 检查是否已存在
            if file_path_str in self.pending_files:
                return False

            # 检查容量限制
            if len(self.pending_files) >= self.config.max_pending_files:
                self.logger.warning(
                    f"待处理文件数达到上限 {self.config.max_pending_files}，跳过文件: {file_path_str}"
                )
                return False

            self.pending_files[file_path_str] = current_time
            return True

    def _remove_from_pending(self, file_path_str: str) -> None:
        """
        从待处理集合中移除文件

        Args:
            file_path_str: 文件路径字符串
        """
        with self.pending_files_lock:
            if file_path_str in self.pending_files:
                del self.pending_files[file_path_str]

    def _start_initial_scan_async(self) -> None:
        """异步执行初始扫描 - 调整后的版本"""

        def scan_task():
            self.initial_scan_in_progress = True
            self.logger.info("开始异步初始扫描...")
            initial_file_count = self.stats["total_files"]
            duplicate_count = 0
            error_count = 0
            processed_count = 0

            try:
                # 初始扫描时检查文件大小，因为文件应该是稳定的
                for file_path, file_size in self.file_scanner.quick_scan_directories(
                    self.config.monitor_directories, check_size=True
                ):
                    if not self.running:
                        break

                    try:
                        file_path_str = str(file_path.resolve())

                        # 1. 检查是否已经在处理中
                        if not self._add_to_pending(file_path_str):
                            duplicate_count += 1
                            continue

                        # 2. 检查是否已处理（在稳定性检查之前）
                        if self._is_file_already_processed(file_path):
                            processed_count += 1
                            self._remove_from_pending(file_path_str)
                            continue

                        file_info = {
                            "file_path": file_path_str,
                            "file_size": file_size,
                            "priority": "normal",
                            "source": "initial_scan",
                            "detected_time": time.time(),
                        }
                        self.raw_file_queue.put(file_info)
                        self.stats["total_files"] += 1
                    except Exception as e:
                        error_count += 1
                        self.logger.warning(f"处理扫描文件失败 {file_path}: {e}")

                new_files = self.stats["total_files"] - initial_file_count
                self.logger.info(
                    f"异步初始扫描完成，找到 {new_files} 个新文件，"
                    f"跳过 {duplicate_count} 个重复文件，"
                    f"{processed_count} 个已处理文件，"
                    f"错误 {error_count} 个文件"
                )

            except Exception as e:
                self.logger.error(f"初始扫描失败: {e}")
                self.logger.debug(f"详细错误: {traceback.format_exc()}")
            finally:
                self.initial_scan_in_progress = False
                self.stats["initial_scan_completed"] = True

        self.initial_scan_thread = threading.Thread(
            target=scan_task, daemon=True, name="InitialScanner"
        )
        self.initial_scan_thread.start()

    def _start_workers(self) -> None:
        """启动工作线程"""
        self.workers = []

        # 启动稳定性检查线程
        stability_threads = max(1, self.config.stability_worker_threads)
        for i in range(stability_threads):
            stability_worker = threading.Thread(
                target=self._stability_worker_process,
                daemon=True,
                name=f"Stability-Worker-{i+1}",
            )
            stability_worker.start()
            self.workers.append(stability_worker)
            self.logger.debug(f"启动稳定性检查线程: Stability-Worker-{i+1}")

        # 启动MD5计算线程
        md5_threads = max(1, self.config.md5_worker_threads)
        for i in range(md5_threads):
            md5_worker = threading.Thread(
                target=self._md5_worker_process, daemon=True, name=f"MD5-Worker-{i+1}"
            )
            md5_worker.start()
            self.workers.append(md5_worker)
            self.logger.debug(f"启动MD5计算线程: MD5-Worker-{i+1}")

        # 启动普通工作线程
        worker_threads = max(1, self.config.worker_threads)
        for i in range(worker_threads):
            worker = threading.Thread(
                target=self._worker_process, daemon=True, name=f"Worker-{i+1}"
            )
            worker.start()
            self.workers.append(worker)
            self.logger.debug(f"启动工作线程: Worker-{i+1}")

        self.logger.info(
            f"工作线程启动完成: "
            f"{stability_threads}个稳定性检查, "
            f"{md5_threads}个MD5计算, "
            f"{worker_threads}个工作线程"
        )

    def _stability_worker_process(self) -> None:
        """稳定性检查工作线程 - 调整后的版本"""
        thread_name = threading.current_thread().name
        self.logger.debug(f"稳定性检查线程开始: {thread_name}")

        while self.running:
            try:
                file_info = self.raw_file_queue.get(timeout=1)
                file_path_str = file_info["file_path"]
                file_path = Path(file_path_str)

                self.logger.debug(f"检查文件稳定性: {file_path}")

                # 进行稳定性检查
                if self._check_file_stability(file_path):
                    # 文件稳定，放入稳定文件队列
                    self.stable_file_queue.put(file_info)
                    self._update_stats("stable_files")
                    self.logger.debug(f"文件稳定: {file_path}")
                else:
                    # 文件不稳定，记录统计并从待处理集合中移除
                    self._update_stats("unstable_files")
                    self._remove_from_pending(file_path_str)
                    self.logger.warning(f"文件不稳定，跳过: {file_path}")

                self.raw_file_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"稳定性检查线程错误: {e}")
                self.logger.debug(f"详细错误: {traceback.format_exc()}")
                # 发生错误时从待处理集合中移除文件
                if "file_path_str" in locals():
                    self._remove_from_pending(file_path_str)
                if not self.raw_file_queue.empty():
                    self.raw_file_queue.task_done()

        self.logger.debug(f"稳定性检查线程结束: {thread_name}")

    def _check_file_stability(self, file_path: Path) -> bool:
        """
        检查文件稳定性 - 调整后的版本
        先进行稳定性检查，然后在稳定性检查后进行最终的文件大小检查
        """
        start_time = time.time()
        max_wait_time = self.config.max_file_wait_time

        # 不再区分小文件和大文件，统一进行稳定性检查
        last_size = -1
        stable_count = 0
        max_stable_checks = 3

        while time.time() - start_time < max_wait_time:
            if not self.running:
                return False

            try:
                if not file_path.exists():
                    self.logger.warning(f"文件在稳定性检查期间消失: {file_path}")
                    return False

                current_size = file_path.stat().st_size

                # 检查文件大小是否稳定
                if current_size == last_size:
                    stable_count += 1
                else:
                    stable_count = 0
                    last_size = current_size

                # 如果连续多次检查大小都相同，认为文件稳定
                if stable_count >= max_stable_checks:
                    # 最后检查文件是否可以访问
                    if self._can_access_file(file_path):
                        # 在稳定性检查之后进行最终的文件大小检查
                        if current_size < self.config.ignore_file_size:
                            formatted_size = format_file_size(current_size)
                            self.logger.info(
                                f"跳过小文件: {file_path} (大小: {formatted_size})"
                            )
                            return False

                        elapsed = time.time() - start_time
                        self.logger.debug(
                            f"文件稳定: {file_path} (等待 {elapsed:.1f} 秒, 大小: {format_file_size(current_size)})"
                        )
                        return True

                # 等待一段时间再检查
                wait_time = min(5, 2 ** (stable_count // 2))  # 指数退避
                time.sleep(wait_time)

            except (OSError, PermissionError) as e:
                self.logger.debug(f"稳定性检查时出错 {file_path}: {e}")
                time.sleep(2)

        self.logger.warning(f"文件稳定性检查超时: {file_path}")
        return False

    def _can_access_file(self, file_path: Path) -> bool:
        """
        检查文件是否可以访问

        Args:
            file_path: 文件路径

        Returns:
            文件是否可访问
        """
        try:
            with open(file_path, "rb") as f:
                f.read(1)  # 尝试读取一个字节
            return True
        except (OSError, PermissionError, IOError) as e:
            self.logger.debug(f"文件无法访问 {file_path}: {e}")
            return False

    def _md5_worker_process(self) -> None:
        """MD5计算工作线程 - 简化版本"""
        thread_name = threading.current_thread().name
        self.logger.debug(f"MD5计算线程开始: {thread_name}")

        while self.running:
            try:
                file_info = self.stable_file_queue.get(timeout=1)
                file_path_str = file_info["file_path"]
                file_path = Path(file_path_str)

                self.logger.debug(f"计算文件MD5: {file_path}")

                # 计算MD5（如果启用）
                md5_hash = None
                if self.config.use_md5:
                    md5_hash = calculate_md5(file_path)
                    if not md5_hash:
                        self.logger.warning(f"无法计算MD5，跳过文件: {file_path}")
                        self._update_stats("md5_failed")
                        self._update_stats("failed_files")
                        self._remove_from_pending(file_path_str)
                        continue

                    self._update_stats("md5_calculated")

                # 使用MD5再次检查是否已处理（更精确的检查）
                if self.config.use_md5 and md5_hash:
                    if self.processed_files_db.is_processed(
                        file_path_str, md5_hash, use_md5=True
                    ):
                        self.logger.debug(f"文件MD5已处理，跳过: {file_path}")
                        self._update_stats("processed_files")
                        self._remove_from_pending(file_path_str)
                        continue

                # 将文件信息放入处理队列
                file_info["md5"] = md5_hash
                self.md5_queue.put(file_info)
                self.logger.debug(f"文件加入处理队列: {file_path}")

                self.stable_file_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"MD5计算线程错误: {e}")
                self.logger.debug(f"详细错误: {traceback.format_exc()}")
                if "file_path_str" in locals():
                    self._remove_from_pending(file_path_str)
                if not self.stable_file_queue.empty():
                    self.stable_file_queue.task_done()

        self.logger.debug(f"MD5计算线程结束: {thread_name}")

    def _worker_process(self) -> None:
        """工作线程处理函数"""
        thread_name = threading.current_thread().name
        self.logger.debug(f"工作线程开始: {thread_name}")

        while self.running:
            try:
                file_info = self.md5_queue.get(timeout=1)

                # 如果初始扫描正在进行且这是低优先级任务，稍微延迟处理
                if self.initial_scan_in_progress and file_info.get("priority") == "low":
                    time.sleep(2)

                start_time = time.time()
                self._process_file(file_info)
                processing_time = time.time() - start_time

                # 更新性能统计
                self._update_performance_stats(processing_time)

                self.md5_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"工作线程错误: {e}")
                self.logger.debug(f"详细错误: {traceback.format_exc()}")
                # 发生错误时从待处理集合中移除文件
                if "file_info" in locals():
                    self._remove_from_pending(file_info["file_path"])
                if not self.md5_queue.empty():
                    self.md5_queue.task_done()

        self.logger.debug(f"工作线程结束: {thread_name}")

    @retry_with_backoff(max_retries=2, initial_delay=2.0)
    def _process_file(self, file_info: Dict[str, Any]) -> None:
        """
        处理单个文件（AI识别、TMDB查询、创建硬链接）

        Args:
            file_info: 文件信息字典
        """
        file_path_str = file_info["file_path"]
        file_path = Path(file_path_str)

        try:
            self.logger.info(f"处理文件: {file_path}")

            # 1. 使用AI提取信息（带熔断器）
            try:
                ai_data = self.ai_circuit_breaker.call(
                    self.ai_processor.extract_media_info, file_path.name
                )
            except Exception as e:
                self.logger.error(
                    f"AI处理失败（熔断器状态: {self.ai_circuit_breaker.state}）: {e}"
                )
                self._update_stats("failed_files")
                return

            if not ai_data:
                self.logger.warning(f"AI无法解析文件: {file_path}")
                self._update_stats("failed_files")
                return

            self.logger.debug(f"AI解析结果: {ai_data}")

            # 2. 查询TMDB（带熔断器）
            try:
                if ai_data["type"] == "movie":
                    tmdb_data = self.tmdb_circuit_breaker.call(
                        self.tmdb_client.search_movie,
                        ai_data["title"],
                        ai_data.get("year"),
                    )
                else:
                    tmdb_data = self.tmdb_circuit_breaker.call(
                        self.tmdb_client.search_tv, ai_data["title"]
                    )
            except Exception as e:
                self.logger.error(
                    f"TMDB查询失败（熔断器状态: {self.tmdb_circuit_breaker.state}）: {e}"
                )
                self._update_stats("failed_files")
                return

            if not tmdb_data:
                self.logger.warning(f"TMDB未找到匹配项: {file_path}")
                self._update_stats("failed_files")
                return

            self.logger.debug(
                f"TMDB搜索结果: {tmdb_data['title']} ({tmdb_data['release_year']})"
            )

            # 3. 判断是否为动漫
            is_anime = tmdb_data.get("is_anime", False)
            self.logger.debug(
                f"TMDB搜索结果: {tmdb_data['title']} ({tmdb_data['release_year']}) - 动漫: {is_anime}"
            )

            # 4. 创建硬链接
            target_path = self.file_linker.organize_file(file_info, tmdb_data, ai_data)
            if target_path:
                # 5. 记录已处理文件
                self.processed_files_db.add_processed_file(
                    file_path_str,
                    file_info["file_size"],
                    file_info["md5"],
                    tmdb_id=tmdb_data["tmdb_id"],
                    media_type=tmdb_data["media_type"],
                    target_path=str(target_path),
                    use_md5=self.config.use_md5,
                )
                self._update_stats("successful_links")
                self.logger.info(f"文件处理完成: {file_path} -> {target_path}")
            else:
                self._update_stats("failed_files")
                self.logger.error(f"创建硬链接失败: {file_path}")

            self._update_stats("processed_files")

        except Exception as e:
            self.logger.error(f"处理文件失败 {file_path}: {e}")
            self.logger.debug(f"详细错误: {traceback.format_exc()}")
            self._update_stats("failed_files")
            raise  # 重新抛出异常以便重试机制工作
        finally:
            self._remove_from_pending(file_path_str)

    def _update_stats(self, stat_key: str) -> None:
        """
        更新统计信息

        Args:
            stat_key: 统计键名
        """
        with threading.Lock():
            self.stats[stat_key] += 1

    def _update_performance_stats(self, processing_time: float) -> None:
        """
        更新性能统计

        Args:
            processing_time: 处理时间
        """
        current_time = time.time()
        self.performance_stats["processing_times"].append(processing_time)

        # 保留最近100个处理时间
        if len(self.performance_stats["processing_times"]) > 100:
            self.performance_stats["processing_times"].pop(0)

        # 定期更新性能统计
        if (
            current_time - self.performance_stats["last_performance_update"]
            >= self.config.performance_monitor_interval
        ):
            if self.performance_stats["processing_times"]:
                avg_time = sum(self.performance_stats["processing_times"]) / len(
                    self.performance_stats["processing_times"]
                )
                self.performance_stats["average_processing_time"] = avg_time

            # 计算每分钟处理文件数
            recent_files = (
                self.stats["processed_files"]
                - self.performance_stats["last_file_count"]
            )
            time_interval = (
                current_time - self.performance_stats["last_performance_update"]
            )
            files_per_minute = (
                (recent_files / time_interval) * 60 if time_interval > 0 else 0
            )

            self.performance_stats["files_processed_per_minute"] = files_per_minute
            self.performance_stats["last_file_count"] = self.stats["processed_files"]
            self.performance_stats["last_performance_update"] = current_time

            self.logger.info(
                f"性能统计 - 平均处理时间: {self.performance_stats['average_processing_time']:.2f}s, "
                f"文件处理速度: {files_per_minute:.2f} 文件/分钟"
            )

    def _cleanup_expired_cache(self) -> None:
        """清理过期缓存"""
        try:
            deleted_count = self.tmdb_cache_db.cleanup_expired()
            if deleted_count > 0:
                self.logger.info(f"清理了 {deleted_count} 个过期TMDB缓存记录")
        except Exception as e:
            self.logger.error(f"清理过期缓存失败: {e}")
            self.logger.debug(f"详细错误: {traceback.format_exc()}")

    def _print_stats(self) -> None:
        """输出统计信息"""
        total_time = time.time() - self.stats["start_time"]
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        self.logger.info("=" * 50)
        self.logger.info("处理统计摘要:")
        self.logger.info(
            f"运行时间: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        )
        self.logger.info(f"总文件数: {self.stats['total_files']}")
        self.logger.info(f"重复文件: {self.stats['duplicate_files']}")
        self.logger.info(f"稳定文件: {self.stats['stable_files']}")
        self.logger.info(f"不稳定文件: {self.stats['unstable_files']}")
        self.logger.info(f"已处理文件: {self.stats['processed_files']}")
        self.logger.info(f"成功链接: {self.stats['successful_links']}")
        self.logger.info(f"失败文件: {self.stats['failed_files']}")
        self.logger.info(f"MD5计算成功: {self.stats['md5_calculated']}")
        self.logger.info(f"MD5计算失败: {self.stats['md5_failed']}")
        self.logger.info(f"初始扫描完成: {self.stats['initial_scan_completed']}")
        self.logger.info(f"当前待处理文件数: {len(self.pending_files)}")

        # 队列状态
        self.logger.info(f"原始文件队列大小: {self.raw_file_queue.qsize()}")
        self.logger.info(f"稳定文件队列大小: {self.stable_file_queue.qsize()}")
        self.logger.info(f"MD5计算队列大小: {self.md5_queue.qsize()}")

        # 性能统计
        self.logger.info(
            f"平均处理时间: {self.performance_stats['average_processing_time']:.2f} 秒"
        )
        self.logger.info(
            f"文件处理速度: {self.performance_stats['files_processed_per_minute']:.2f} 文件/分钟"
        )

        # 熔断器状态
        self.logger.info(f"AI熔断器状态: {self.ai_circuit_breaker.state.value}")
        self.logger.info(f"TMDB熔断器状态: {self.tmdb_circuit_breaker.state.value}")

        self.logger.info("=" * 50)

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态信息

        Returns:
            系统状态字典
        """
        # 获取健康状态
        health_status = self.health_monitor.get_health_status()

        status = {
            "running": self.running,
            "initial_scan_completed": self.stats["initial_scan_completed"],
            "initial_scan_in_progress": self.initial_scan_in_progress,
            "queues": {
                "raw_files": self.raw_file_queue.qsize(),
                "stable_files": self.stable_file_queue.qsize(),
                "md5_files": self.md5_queue.qsize(),
            },
            "threads": {
                "active": sum(1 for worker in self.workers if worker.is_alive()),
                "total": len(self.workers),
            },
            "pending_files": len(self.pending_files),
            "performance": self.performance_stats,
            "health": health_status,
            "circuit_breakers": {
                "ai": self.ai_circuit_breaker.get_status(),
                "tmdb": self.tmdb_circuit_breaker.get_status(),
            },
        }

        return status

    def start_monitoring(self) -> None:
        """开始监控模式 - 支持配置热重载"""
        self.logger.info("启动媒体文件整理监控器...")

        # 设置高级日志
        try:
            setup_advanced_logging(
                level=self.config.log_level,
                log_file="logs/media_organizer.log",
                max_log_size=50,
                backup_count=10,
            )
        except Exception as e:
            self.logger.error(f"设置日志失败: {e}")
            # 使用基本日志配置
            import logging

            logging.basicConfig(
                level=getattr(logging, self.config.log_level.upper(), logging.INFO)
            )

        # 验证配置
        try:
            self.config.validate_config()
        except ConfigValidationError as e:
            self.logger.error(f"配置验证失败: {e}")
            return

        # 记录脱敏的配置信息
        safe_config = self.config.sanitized_config_for_logging()
        self.logger.info(f"运行配置: {safe_config}")

        self.running = True

        try:
            # 设置信号处理
            self._setup_signal_handlers()

            # 启动健康监控
            self._setup_health_checks()

            # 1. 首先启动工作线程
            self._start_workers()
            self.logger.info("工作线程已启动")

            # 2. 启动文件监控器
            self.file_monitor.start()
            self.logger.info("文件监控器已启动")

            # 3. 如果需要，异步执行初始扫描
            if self.config.initial_scan:
                self._start_initial_scan_async()
                self.logger.info("初始扫描已开始（异步）")
            else:
                self.logger.info("跳过初始扫描")
                self.stats["initial_scan_completed"] = True

            self.logger.info("媒体文件整理监控器已完全启动，按 Ctrl+C 停止")

            # 主循环 - 定期检查和维护
            last_cache_cleanup = time.time()
            last_status_log = time.time()
            last_health_check = time.time()
            last_config_check = time.time()

            while self.running:
                time.sleep(5)  # 降低主循环频率
                current_time = time.time()

                # 定期检查配置变化（每30秒）
                if current_time - last_config_check >= 30:
                    if self.config._should_reload():
                        self.logger.info("检测到配置文件变化，重新加载配置...")
                        try:
                            old_log_level = self.config.log_level
                            self.config.load_config()
                            self.config.validate_config()

                            # 更新动态配置
                            self._update_dynamic_config()

                            self.logger.info("配置重载成功")
                        except Exception as e:
                            self.logger.error(f"配置重载失败: {e}")

                    last_config_check = current_time

                # 定期清理过期缓存（每天一次）
                if current_time - last_cache_cleanup >= 86400:  # 24小时
                    self._cleanup_expired_cache()
                    last_cache_cleanup = current_time

                # 定期输出状态信息（每5分钟一次）
                if current_time - last_status_log >= 300:  # 5分钟
                    status = self.get_system_status()
                    self.logger.info(
                        f"系统状态 - "
                        f"队列: {status['queues']}, "
                        f"线程: {status['threads']['active']}/{status['threads']['total']}, "
                        f"待处理: {status['pending_files']}"
                    )
                    last_status_log = current_time

                # 定期检查健康状态（每2分钟一次）
                if current_time - last_health_check >= 120:  # 2分钟
                    if not self.health_monitor.is_healthy():
                        unhealthy_components = (
                            self.health_monitor.get_unhealthy_components()
                        )
                        health_status = self.health_monitor.get_health_status()

                        self.logger.warning(
                            f"系统健康状态异常，不健康的组件: {unhealthy_components}"
                        )

                        # 输出详细的健康状态信息
                        for component in unhealthy_components:
                            if component in health_status:
                                component_status = health_status[component]
                                self.logger.warning(
                                    f"组件 '{component}' 状态: {component_status}"
                                )

                                # 输出详细信息
                                if "details" in component_status:
                                    for key, detail in component_status[
                                        "details"
                                    ].items():
                                        if detail.get("status") != "healthy":
                                            self.logger.warning(f"  - {key}: {detail}")
                    else:
                        self.logger.debug("系统健康状态正常")

                    last_health_check = current_time

                # 检查健康状态
                if not self.health_monitor.is_healthy():
                    unhealthy_components = (
                        self.health_monitor.get_unhealthy_components()
                    )
                    self.logger.warning(
                        f"系统健康状态异常，不健康的组件: {unhealthy_components}"
                    )

        except KeyboardInterrupt:
            self.logger.info("收到停止信号，正在关闭...")
        except Exception as e:
            self.logger.error(f"监控模式错误: {e}")
            self.logger.debug(f"详细错误: {traceback.format_exc()}")
        finally:
            self.stop()

    def _setup_signal_handlers(self) -> None:
        """设置信号处理"""

        def signal_handler(signum, frame):
            self.logger.info(f"收到信号 {signum}，正在优雅关闭...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def stop(self) -> None:
        """停止所有组件 - 修复资源清理"""
        self.logger.info("正在停止媒体文件整理监控器...")
        self.running = False

        # 停止文件监控器
        try:
            self.file_monitor.stop()
            self.logger.info("文件监控器已停止")
        except Exception as e:
            self.logger.error(f"停止文件监控器失败: {e}")

        # 停止健康监控
        try:
            self.health_monitor.stop()
            self.logger.info("健康监控器已停止")
        except Exception as e:
            self.logger.error(f"停止健康监控器失败: {e}")

        # 等待工作线程完成
        self.logger.info("等待工作线程完成...")
        for worker in self.workers:
            worker.join(timeout=5)

        # 清理数据库连接池（重要！）
        try:
            if hasattr(self, "tmdb_cache_db"):
                self.tmdb_cache_db.connection_pool.close_all()
            if hasattr(self, "processed_files_db"):
                self.processed_files_db.connection_pool.close_all()
            self.logger.info("数据库连接池已关闭")
        except Exception as e:
            self.logger.error(f"关闭数据库连接池失败: {e}")

        # 输出最终统计
        self._print_stats()
        self.logger.info("媒体文件整理监控器已停止")


def main():
    """主函数"""
    try:
        # 加载配置
        config = Config()

        # 创建整理器
        organizer = MediaOrganizer(config)

        # 启动监控模式
        organizer.start_monitoring()

    except ConfigValidationError as e:
        logger.error(f"配置验证失败: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序运行错误: {e}")
        logger.debug(f"详细错误: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
