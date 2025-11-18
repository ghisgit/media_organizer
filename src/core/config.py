import configparser
import os
from pathlib import Path
from typing import List, Dict, Any
import logging
import time
import threading

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """配置验证错误"""

    pass


class Config:
    """配置管理类 - 支持热重载"""

    def __init__(
        self, config_file: str = "config.ini", enable_auto_reload: bool = True
    ):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self._validation_errors: List[str] = []
        self._last_mtime = 0
        self._lock = threading.RLock()
        self._enable_auto_reload = enable_auto_reload

        # 初始加载配置
        self.load_config()
        self.validate_config()

        # 启动自动重载线程
        if self._enable_auto_reload and self.auto_reload:
            self._start_auto_reload()

    def _start_auto_reload(self):
        """启动自动重载线程"""

        def reload_monitor():
            while True:
                try:
                    time.sleep(10)
                    if self._should_reload():
                        logger.info("检测到配置文件变化，重新加载配置...")
                        self.load_config()
                        self.validate_config()
                except Exception as e:
                    logger.error(f"配置重载监控错误: {e}")
                    time.sleep(30)

        reload_thread = threading.Thread(
            target=reload_monitor, daemon=True, name="ConfigReloadMonitor"
        )
        reload_thread.start()
        logger.info("配置自动重载监控已启动")

    def _should_reload(self) -> bool:
        """检查是否需要重新加载配置"""
        try:
            if not os.path.exists(self.config_file):
                return False

            current_mtime = os.path.getmtime(self.config_file)
            if current_mtime > self._last_mtime:
                self._last_mtime = current_mtime
                return True
        except Exception as e:
            logger.error(f"检查配置文件修改时间失败: {e}")

        return False

    def load_config(self) -> None:
        """加载配置文件"""
        with self._lock:
            if not os.path.exists(self.config_file):
                logger.warning(f"配置文件不存在，创建默认配置: {self.config_file}")
                self.create_default_config()

            try:
                self._last_mtime = os.path.getmtime(self.config_file)
                self.config.read(self.config_file, encoding="utf-8")
                logger.info(f"成功加载配置文件: {self.config_file}")
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}")
                raise ConfigValidationError(f"无法加载配置文件: {e}")

    def reload_config(self) -> bool:
        """手动重新加载配置"""
        try:
            self.load_config()
            self.validate_config()
            logger.info("手动重载配置成功")
            return True
        except Exception as e:
            logger.error(f"手动重载配置失败: {e}")
            return False

    def create_default_config(self) -> None:
        """创建默认配置文件"""
        config_sections = {
            "PATHS": {
                "# 要监控的目录，多个目录用逗号分隔": "",
                "monitor_directories": "/path/to/movies,/path/to/tv_shows",
                "# 媒体库根目录": "",
                "library_path": "/path/to/media_library",
                "# 动漫目录名称": "",
                "anime_directory": "动漫",
            },
            "AI": {
                "# AI服务类型: deepseek, spark, model_scope, zhipu": "",
                "ai_type": "deepseek",
                "# AI并发请求限制": "",
                "ai_max_concurrent": "5",
                "# AI输出token限制（默认200，足够完成媒体信息提取）": "",
                "ai_max_tokens": "200",
                "# DeepSeek API配置": "",
                "deepseek_api_key": "your_deepseek_api_key",
                "deepseek_url": "https://api.deepseek.com/v1/chat/completions",
                "# 讯飞星火认知大模型配置": "",
                "spark_api_key": "your_spark_api_key",
                "spark_url": "https://spark-api-open.xf-yun.com/v1/chat/completions",
                "spark_model": "Lite",
                "# 魔塔API-Inference配置": "",
                "model_scope_api_key": "your_model_scope_api_key",
                "model_scope_url": "https://api-inference.modelscope.cn/v1/chat/completions",
                "model_scope_model": "Qwen3-235B-A22B-Instruct-2507",
                "# 智普AI配置": "",
                "zhipu_api_key": "your_zhipu_api_key",
                "zhipu_url": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
                "zhipu_model": "GLM-4.5-Flash",
            },
            "TMDB": {
                "# TMDB API配置": "",
                "tmdb_api_key": "your_tmdb_api_key",
                "# TMDB请求代理（可选）": "",
                "tmdb_proxy": "",
            },
            "DATABASE": {
                "# 数据库文件路径": "",
                "tmdb_cache_db": "tmdb_cache.db",
                "processed_files_db": "processed_files.db",
            },
            "SYSTEM": {
                "# 工作线程数": "",
                "worker_threads": "5",
                "# 稳定性检查线程数": "",
                "stability_worker_threads": "2",
                "# MD5计算线程数": "",
                "md5_worker_threads": "2",
                "# 日志级别: DEBUG, INFO, WARNING, ERROR": "",
                "log_level": "INFO",
                "# 初始扫描模式 (true/false)": "",
                "initial_scan": "true",
                "# 监控文件事件类型: created, moved": "",
                "watch_events": "created,moved",
                "# 文件稳定延迟（秒）": "",
                "file_stable_delay": "5",
                "# 忽略的文件模式": "",
                "ignore_patterns": "*.tmp,*.part,*.crdownload,*.swp",
                "# 文件稳定检查最大等待时间（秒）": "",
                "max_file_wait_time": "300",
                "# 忽略的文件大小（MB）": "",
                "ignore_file_size": "10",
                "# 文件访问重试间隔（秒）": "",
                "file_retry_interval": "5",
                "# 最大待处理文件数": "",
                "max_pending_files": "10000",
                "# 性能监控间隔（秒）": "",
                "performance_monitor_interval": "60",
                "# MD5检查开关 (true/false)": "",
                "use_md5": "true",
                "# 文件链接方法: hardlink, symlink, copy": "",
                "link_method": "hardlink",
                "# 配置自动重载 (true/false)": "",
                "auto_reload": "true",
            },
        }

        for section, options in config_sections.items():
            self.config[section] = options

        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                self.config.write(f)
            logger.info(f"已创建默认配置文件: {self.config_file}")
        except Exception as e:
            logger.error(f"创建默认配置文件失败: {e}")
            raise

    def _get_path_list(self, key: str, default: str) -> List[Path]:
        """获取路径列表"""
        with self._lock:
            dirs = self.config["PATHS"].get(key, default).split(",")
            return [Path(d.strip()) for d in dirs if d.strip()]

    def _get_int(self, section: str, key: str, default: int) -> int:
        """获取整数值"""
        with self._lock:
            try:
                return int(self.config[section].get(key, str(default)))
            except (ValueError, KeyError):
                self._validation_errors.append(
                    f"配置项 {section}.{key} 值无效，使用默认值: {default}"
                )
                return default

    def _get_float(self, section: str, key: str, default: float) -> float:
        """获取浮点数值"""
        with self._lock:
            try:
                return float(self.config[section].get(key, str(default)))
            except (ValueError, KeyError):
                self._validation_errors.append(
                    f"配置项 {section}.{key} 值无效，使用默认值: {default}"
                )
                return default

    def _get_bool(self, section: str, key: str, default: bool) -> bool:
        """获取布尔值"""
        with self._lock:
            try:
                value = self.config[section].get(key, str(default)).lower()
                return value in ("true", "yes", "1", "on")
            except KeyError:
                self._validation_errors.append(
                    f"配置项 {section}.{key} 值无效，使用默认值: {default}"
                )
                return default

    def _get_str_list(self, section: str, key: str, default: str) -> List[str]:
        """获取字符串列表"""
        with self._lock:
            try:
                items = self.config[section].get(key, default)
                return [item.strip() for item in items.split(",") if item.strip()]
            except KeyError:
                self._validation_errors.append(
                    f"配置项 {section}.{key} 值无效，使用默认值: {default}"
                )
                return [item.strip() for item in default.split(",") if item.strip()]

    def validate_config(self) -> None:
        """验证配置"""
        errors = []

        # 检查必要配置
        if not self.tmdb_api_key or self.tmdb_api_key == "your_tmdb_api_key":
            errors.append("TMDB API密钥未配置")

        if not self.monitor_directories:
            errors.append("未配置监控目录")

        # 验证媒体库目录
        try:
            if not self.library_path.exists():
                try:
                    self.library_path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"创建媒体库目录: {self.library_path}")
                except Exception as e:
                    errors.append(
                        f"媒体库目录不存在且无法创建: {self.library_path} - {e}"
                    )
        except Exception as e:
            errors.append(f"媒体库目录验证失败: {e}")

        # 记录验证错误
        for error in errors:
            logger.error(f"配置验证错误: {error}")

        for warning in self._validation_errors:
            logger.warning(warning)

        if errors:
            raise ConfigValidationError(f"配置验证失败: {'; '.join(errors)}")

    def sanitized_config_for_logging(self) -> Dict[str, Any]:
        """返回脱敏的配置信息（用于日志）"""
        with self._lock:
            safe_config = {}

            for section in self.config.sections():
                safe_config[section] = {}
                for key, value in self.config[section].items():
                    if any(
                        sensitive in key.lower()
                        for sensitive in ["key", "password", "token", "secret", "auth"]
                    ):
                        if value and value != f"your_{key}":
                            safe_config[section][key] = "***"
                        else:
                            safe_config[section][key] = "未设置"
                    else:
                        safe_config[section][key] = value

            return safe_config

    # 路径相关属性
    @property
    def monitor_directories(self) -> List[Path]:
        return self._get_path_list("monitor_directories", "")

    @property
    def library_path(self) -> Path:
        with self._lock:
            return Path(self.config["PATHS"].get("library_path", "./media_library"))

    @property
    def anime_directory(self) -> str:
        with self._lock:
            return self.config["PATHS"].get("anime_directory", "动漫")

    # AI相关属性
    @property
    def ai_type(self) -> str:
        with self._lock:
            return self.config["AI"].get("ai_type", "deepseek")

    @property
    def ai_max_concurrent(self) -> int:
        return self._get_int("AI", "ai_max_concurrent", 5)

    @property
    def ai_max_tokens(self) -> int:
        return self._get_int("AI", "ai_max_tokens", 200)

    @property
    def deepseek_api_key(self) -> str:
        with self._lock:
            return self.config["AI"].get("deepseek_api_key", "")

    @property
    def deepseek_url(self) -> str:
        with self._lock:
            return self.config["AI"].get(
                "deepseek_url", "https://api.deepseek.com/v1/chat/completions"
            )

    @property
    def spark_api_key(self) -> str:
        with self._lock:
            return self.config["AI"].get("spark_api_key", "")

    @property
    def spark_url(self) -> str:
        with self._lock:
            return self.config["AI"].get(
                "spark_url", "https://spark-api-open.xf-yun.com/v1/chat/completions"
            )

    @property
    def spark_model(self) -> str:
        with self._lock:
            return self.config["AI"].get("spark_model", "Lite")

    @property
    def model_scope_api_key(self) -> str:
        with self._lock:
            return self.config["AI"].get("model_scope_api_key", "")

    @property
    def model_scope_url(self) -> str:
        with self._lock:
            return self.config["AI"].get(
                "model_scope_url",
                "https://api-inference.modelscope.cn/v1/chat/completions",
            )

    @property
    def model_scope_model(self) -> str:
        with self._lock:
            return self.config["AI"].get(
                "model_scope_model", "Qwen3-235B-A22B-Instruct-2507"
            )

    @property
    def zhipu_api_key(self) -> str:
        with self._lock:
            return self.config["AI"].get("zhipu_api_key", "")

    @property
    def zhipu_url(self) -> str:
        with self._lock:
            return self.config["AI"].get(
                "zhipu_url", "https://open.bigmodel.cn/api/paas/v4/chat/completions"
            )

    @property
    def zhipu_model(self) -> str:
        with self._lock:
            return self.config["AI"].get("zhipu_model", "GLM-4.5-Flash")

    # TMDB相关属性
    @property
    def tmdb_api_key(self) -> str:
        with self._lock:
            return self.config["TMDB"].get("tmdb_api_key", "")

    @property
    def tmdb_proxy(self) -> str:
        with self._lock:
            return self.config["TMDB"].get("tmdb_proxy", "")

    @property
    def cache_expire_days(self) -> int:
        return self._get_int("TMDB", "cache_expire_days", 30)

    # 数据库相关属性
    @property
    def tmdb_cache_db(self) -> str:
        with self._lock:
            return self.config["DATABASE"].get("tmdb_cache_db", "tmdb_cache.db")

    @property
    def processed_files_db(self) -> str:
        with self._lock:
            return self.config["DATABASE"].get(
                "processed_files_db", "processed_files.db"
            )

    # 系统相关属性
    @property
    def worker_threads(self) -> int:
        return max(1, self._get_int("SYSTEM", "worker_threads", 5))

    @property
    def stability_worker_threads(self) -> int:
        return max(1, self._get_int("SYSTEM", "stability_worker_threads", 2))

    @property
    def md5_worker_threads(self) -> int:
        return max(1, self._get_int("SYSTEM", "md5_worker_threads", 2))

    @property
    def log_level(self) -> str:
        with self._lock:
            return self.config["SYSTEM"].get("log_level", "INFO")

    @property
    def initial_scan(self) -> bool:
        return self._get_bool("SYSTEM", "initial_scan", True)

    @property
    def watch_events(self) -> List[str]:
        return self._get_str_list("SYSTEM", "watch_events", "created,moved")

    @property
    def file_stable_delay(self) -> int:
        return self._get_int("SYSTEM", "file_stable_delay", 5)

    @property
    def ignore_patterns(self) -> List[str]:
        return self._get_str_list(
            "SYSTEM", "ignore_patterns", "*.tmp,*.part,*.crdownload,*.swp"
        )

    @property
    def max_file_wait_time(self) -> int:
        return self._get_int("SYSTEM", "max_file_wait_time", 300)

    @property
    def ignore_file_size(self) -> int:
        mb_size = self._get_int("SYSTEM", "ignore_file_size", 10)
        return mb_size * 1024 * 1024

    @property
    def file_retry_interval(self) -> int:
        return self._get_int("SYSTEM", "file_retry_interval", 5)

    @property
    def max_pending_files(self) -> int:
        return self._get_int("SYSTEM", "max_pending_files", 10000)

    @property
    def performance_monitor_interval(self) -> int:
        return self._get_int("SYSTEM", "performance_monitor_interval", 60)

    @property
    def use_md5(self) -> bool:
        return self._get_bool("SYSTEM", "use_md5", True)

    @property
    def link_method(self) -> str:
        with self._lock:
            return self.config["SYSTEM"].get("link_method", "hardlink")

    @property
    def auto_reload(self) -> bool:
        return self._get_bool("SYSTEM", "auto_reload", True)
