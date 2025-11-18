import argparse
import sys
from pathlib import Path
from typing import Optional

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.core.config import Config
from src.core.media_organizer import MediaOrganizer
from src.utils.helpers import setup_logging
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class CommandLineOrganizer:
    """命令行模式整理器"""

    def __init__(self, config: Config, test_mode: bool = False):
        self.config = config
        self.test_mode = test_mode
        self.logger = logger

        # 初始化组件
        self._init_components()

    def _init_components(self):
        """初始化组件"""
        try:
            from src.core.database import TMDBCacheDB, ProcessedFilesDB
            from src.processors.ai_processor import AIProcessor
            from src.processors.tmdb_client import TMDBClient
            from src.linkers.file_linker import FileLinker

            self.tmdb_cache_db = TMDBCacheDB(
                self.config.tmdb_cache_db, self.config.cache_expire_days
            )
            self.processed_files_db = ProcessedFilesDB(self.config.processed_files_db)
            self.ai_processor = AIProcessor(self.config)
            self.tmdb_client = TMDBClient(
                self.config.tmdb_api_key, self.tmdb_cache_db, self.config.tmdb_proxy
            )
            self.file_linker = FileLinker(
                self.config.library_path, self.config.anime_directory
            )

            if self.test_mode:
                self.logger.info("测试模式已启用")
            else:
                self.logger.info("正常模式")

        except Exception as e:
            self.logger.error(f"初始化组件失败: {e}")
            raise

    def organize_single_file(self, file_path: Path) -> bool:
        """整理单个文件"""
        try:
            self.logger.info(f"处理文件: {file_path}")

            # 基本检查
            if not file_path.exists() or not file_path.is_file():
                self.logger.error(f"文件无效: {file_path}")
                return False

            from src.utils.helpers import is_video_file, calculate_md5, format_file_size

            if not is_video_file(file_path):
                self.logger.error(f"不是视频文件: {file_path}")
                return False

            file_size = file_path.stat().st_size
            if file_size < self.config.ignore_file_size:
                self.logger.warning(f"文件太小: {file_path}")
                return False

            # 检查是否已处理
            md5_hash = calculate_md5(file_path) if self.config.use_md5 else None
            if not self.test_mode and self.processed_files_db.is_processed(
                str(file_path), md5_hash, self.config.use_md5
            ):
                self.logger.info(f"文件已处理: {file_path}")
                return True

            # 处理文件
            if self._process_file(file_path, file_size, md5_hash):
                self.logger.info(f"文件处理完成: {file_path}")
                return True
            else:
                self.logger.error(f"文件处理失败: {file_path}")
                return False

        except Exception as e:
            self.logger.error(f"处理文件失败 {file_path}: {e}")
            return False

    def _process_file(
        self, file_path: Path, file_size: int, md5_hash: Optional[str]
    ) -> bool:
        """处理文件核心逻辑"""
        # AI提取信息
        ai_data = self.ai_processor.extract_media_info(file_path.name)
        if not ai_data:
            self.logger.error(f"AI解析失败: {file_path.name}")
            return False

        # TMDB查询
        if ai_data["type"] == "movie":
            tmdb_data = self.tmdb_client.search_movie(
                ai_data["title"], ai_data.get("year")
            )
        else:
            tmdb_data = self.tmdb_client.search_tv(ai_data["title"])

        if not tmdb_data:
            self.logger.error(f"TMDB未找到: {ai_data['title']}")
            return False

        # 创建链接
        file_info = {"file_path": str(file_path), "file_size": file_size}
        target_path = self.file_linker.organize_file(file_info, tmdb_data, ai_data)

        if not target_path:
            self.logger.error(f"创建链接失败: {file_path}")
            return False

        # 记录到数据库
        if not self.test_mode:
            self.processed_files_db.add_processed_file(
                str(file_path),
                file_size,
                md5_hash,
                tmdb_id=tmdb_data["tmdb_id"],
                media_type=tmdb_data["media_type"],
                target_path=str(target_path),
                use_md5=self.config.use_md5,
            )

        return True


def main():
    """命令行模式主函数"""
    parser = argparse.ArgumentParser(
        description="媒体文件整理器 - 命令行模式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 整理单个文件
  python cli.py --file /path/to/movie.mp4
  
  # 整理目录
  python cli.py --dir /path/to/media
  
  # 测试模式
  python cli.py --file /path/to/movie.mp4 --test
  
  # 使用特定配置
  python cli.py --dir /path/to/media --config /path/to/config.ini
        """,
    )

    # 输入选项
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--file", nargs="+", help="要整理的文件")
    input_group.add_argument("--dir", help="要整理的目录")

    # 其他选项
    parser.add_argument("--config", default="config.ini", help="配置文件路径")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    parser.add_argument("--test", action="store_true", help="测试模式")

    args = parser.parse_args()

    try:
        # 加载配置
        config = Config(args.config)

        # 设置日志
        log_level = "DEBUG" if args.verbose else config.log_level
        setup_logging(log_level)

        # 创建整理器
        organizer = CommandLineOrganizer(config, test_mode=args.test)

        # 处理文件或目录
        if args.file:
            success_count = 0
            for file_path_str in args.file:
                file_path = Path(file_path_str)
                if organizer.organize_single_file(file_path):
                    success_count += 1

            total_count = len(args.file)
            print(f"处理完成: {success_count}/{total_count} 成功")

        elif args.dir:
            # 这里可以添加目录处理逻辑
            print(f"目录处理功能待实现: {args.dir}")

        sys.exit(0 if success_count == len(args.file) else 1)

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
