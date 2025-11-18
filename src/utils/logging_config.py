import logging
import logging.handlers
import sys
import re
from pathlib import Path


class SensitiveDataFilter(logging.Filter):
    """敏感数据过滤器"""

    SENSITIVE_KEYS = ["key", "password", "token", "secret", "api_key", "auth"]

    def filter(self, record: logging.LogRecord) -> bool:
        """过滤敏感信息"""
        if hasattr(record, "msg") and record.msg:
            record.msg = self._sanitize_message(str(record.msg))

        if hasattr(record, "args") and record.args:
            if isinstance(record.args, dict):
                record.args = self._sanitize_dict(record.args)

        return True

    def _sanitize_message(self, message: str) -> str:
        """清理消息中的敏感信息"""
        # 替换API密钥模式
        message = re.sub(
            r"api[_-]?key=([^\s&]+)", "api_key=***", message, flags=re.IGNORECASE
        )
        message = re.sub(
            r"password=([^\s&]+)", "password=***", message, flags=re.IGNORECASE
        )
        message = re.sub(r"token=([^\s&]+)", "token=***", message, flags=re.IGNORECASE)

        return message

    def _sanitize_dict(self, data: dict) -> dict:
        """清理字典中的敏感信息"""
        sanitized = {}
        for key, value in data.items():
            if any(sensitive in str(key).lower() for sensitive in self.SENSITIVE_KEYS):
                sanitized[key] = "***"
            else:
                sanitized[key] = value
        return sanitized


def setup_advanced_logging(
    level: str = "INFO",
    log_file: str = "logs/media_organizer.log",
    max_log_size: int = 50,
    backup_count: int = 5,
    enable_console: bool = True,
) -> None:
    """设置高级日志配置"""
    # 创建日志目录
    log_path = Path(log_file)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"创建日志目录失败: {e}")
        log_file = "media_organizer.log"
        log_path = Path(log_file)

    # 获取日志级别
    log_level = getattr(logging, level.upper(), logging.INFO)

    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 移除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    # 文件处理器
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_log_size * 1024 * 1024,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(SensitiveDataFilter())
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"创建文件日志处理器失败: {e}")
        enable_console = True

    # 控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(SensitiveDataFilter())
        root_logger.addHandler(console_handler)

    # 设置第三方库日志级别
    logging.getLogger("watchdog").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"日志系统初始化完成 - 级别: {level}")


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志记录器"""
    return logging.getLogger(name)
