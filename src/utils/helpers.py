import hashlib
import logging
import time
from pathlib import Path
from typing import Optional, Callable
from functools import wraps

# 视频文件扩展名
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".m4v",
    ".mpg",
    ".mpeg",
    ".rm",
    ".rmvb",
    ".ts",
    ".m2ts",
    ".3gp",
    ".asf",
    ".f4v",
    ".m2t",
    ".m2ts",
    ".mts",
    ".ogv",
    ".qt",
    ".vob",
    ".dat",
}


def setup_logging(level: str = "INFO") -> None:
    """设置基本日志"""
    log_level = getattr(logging, level.upper(), logging.INFO)

    # 创建日志目录
    log_dir = Path("logs")
    try:
        log_dir.mkdir(exist_ok=True)
    except Exception as e:
        print(f"创建日志目录失败: {e}")
        log_dir = Path(".")

    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 文件处理器
    try:
        file_handler = logging.FileHandler(
            log_dir / "media_organizer.log", encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
    except Exception as e:
        print(f"创建文件日志处理器失败: {e}")
        file_handler = None

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 配置根日志记录器
    handlers = [console_handler]
    if file_handler:
        handlers.append(file_handler)

    logging.basicConfig(level=log_level, handlers=handlers)


def calculate_md5(file_path: Path, max_retries: int = 3) -> Optional[str]:
    """计算文件的MD5值"""
    logger = logging.getLogger(__name__)

    for attempt in range(max_retries):
        try:
            file_size = file_path.stat().st_size
            if file_size == 0:
                logger.debug(f"文件大小为0: {file_path}")
                return None

            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)

            md5_result = hash_md5.hexdigest()
            logger.debug(f"MD5计算成功: {file_path}")
            return md5_result

        except (FileNotFoundError, PermissionError, OSError) as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"MD5计算失败，重试 {attempt + 1}/{max_retries}: {file_path}"
                )
                time.sleep(2)
            else:
                logger.error(f"MD5计算最终失败 {file_path}: {e}")
                return None
        except Exception as e:
            logger.error(f"计算MD5失败 {file_path}: {e}")
            return None

    return None


def is_video_file(file_path: Path) -> bool:
    """检查是否是视频文件"""
    is_video = file_path.suffix.lower() in VIDEO_EXTENSIONS
    if not is_video:
        logging.debug(f"不是视频文件: {file_path}")
    return is_video


def safe_file_operation(func: Callable) -> Callable:
    """文件操作安全装饰器"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except PermissionError as e:
            logging.error(f"文件被占用或无权限: {e}")
            return False
        except OSError as e:
            logging.error(f"文件操作失败: {e}")
            return False
        except Exception as e:
            logging.error(f"未知错误: {e}")
            return False

    return wrapper


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小"""
    if size_bytes == 0:
        return "0B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1

    return f"{size_bytes:.2f} {size_names[i]}"
