import logging
import sys
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# 设置基本日志配置
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """主函数"""
    try:
        from src.core.config import Config
        from src.core.media_organizer import MediaOrganizer
        from src.utils.logging_config import setup_advanced_logging

        # 加载配置
        config = Config()

        # 设置高级日志
        setup_advanced_logging(config.log_level)

        logger.info("启动媒体文件整理器...")

        # 创建整理器
        organizer = MediaOrganizer(config)

        # 启动监控模式
        organizer.start_monitoring()

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
