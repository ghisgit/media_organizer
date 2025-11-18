import logging
import os
import shutil
from pathlib import Path
from typing import Optional, Dict, Any
from ..utils.helpers import safe_file_operation


class FileLinker:
    """文件链接器 - 负责文件组织和链接创建"""

    def __init__(
        self, library_path: Path, anime_directory: str, link_method: str = "hardlink"
    ):
        self.library_path = library_path
        self.anime_directory = anime_directory
        self.link_method = link_method
        self.logger = logging.getLogger(__name__)

        self._create_library_structure()

    def _create_library_structure(self):
        """创建媒体库目录结构"""
        directories = [
            self.library_path / "电影",
            self.library_path / "电视",
            self.library_path / self.anime_directory / "电影",
            self.library_path / self.anime_directory / "电视",
        ]

        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"创建目录: {directory}")
            except Exception as e:
                self.logger.error(f"创建目录失败 {directory}: {e}")

    def _sanitize_filename(self, name: str) -> str:
        """清理文件名"""
        invalid_chars = ["<", ">", ":", '"', "/", "\\", "|", "?", "*"]
        for char in invalid_chars:
            name = name.replace(char, "")
        return name.strip()

    def _get_target_path(
        self,
        media_info: Dict[str, Any],
        tmdb_data: Dict[str, Any],
        is_anime: bool,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> Path:
        """获取目标路径"""
        title = self._sanitize_filename(tmdb_data["title"])
        year = tmdb_data["release_year"]
        file_suffix = Path(media_info["file_path"]).suffix

        if tmdb_data["media_type"] == "movie":
            base_dir = self._get_base_dir("movie", is_anime)
            folder_name = f"{title} ({year})"
            file_name = f"{title} ({year}){file_suffix}"
            return base_dir / folder_name / file_name
        else:
            base_dir = self._get_base_dir("tv", is_anime)
            folder_name = f"{title} ({year})"
            season_folder = f"Season {season:02d}"
            file_name = f"{title} S{season:02d}E{episode:02d}{file_suffix}"
            return base_dir / folder_name / season_folder / file_name

    def _get_base_dir(self, media_type: str, is_anime: bool) -> Path:
        """获取基础目录"""
        if is_anime:
            return (
                self.library_path
                / self.anime_directory
                / ("电影" if media_type == "movie" else "电视")
            )
        else:
            return self.library_path / ("电影" if media_type == "movie" else "电视")

    @safe_file_operation
    def create_link(
        self, source_path: Path, target_path: Path, method: Optional[str] = None
    ) -> bool:
        """创建文件链接"""
        if method is None:
            method = self.link_method

        if not source_path.exists():
            self.logger.error(f"源文件不存在: {source_path}")
            return False

        # 目标文件已存在，视为已处理
        if target_path.exists():
            self.logger.info(f"目标文件已存在: {target_path}")
            return True

        # 创建目标目录
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if method == "hardlink":
                return self._create_hardlink(source_path, target_path)
            elif method == "symlink":
                return self._create_symlink(source_path, target_path)
            elif method == "copy":
                return self._copy_file(source_path, target_path)
            else:
                self.logger.error(f"不支持的链接方法: {method}")
                return False
        except Exception as e:
            self.logger.error(f"创建{method}失败: {e}")
            return False

    def _create_hardlink(self, source_path: Path, target_path: Path) -> bool:
        """创建硬链接"""
        try:
            os.link(str(source_path), str(target_path))
            self.logger.info(f"硬链接创建成功: {source_path} -> {target_path}")
            return True
        except OSError as e:
            self.logger.error(f"创建硬链接失败: {e}")
            if e.errno == 18:  # 跨文件系统
                self.logger.warning("跨文件系统，尝试符号链接")
                return self._create_symlink(source_path, target_path)
            return False

    def _create_symlink(self, source_path: Path, target_path: Path) -> bool:
        """创建符号链接"""
        try:
            absolute_source = source_path.resolve()
            target_path.symlink_to(absolute_source)
            self.logger.info(f"符号链接创建成功: {absolute_source} -> {target_path}")
            return True
        except OSError as e:
            self.logger.error(f"创建符号链接失败: {e}")
            self.logger.warning("符号链接失败，尝试复制文件")
            return self._copy_file(source_path, target_path)

    def _copy_file(self, source_path: Path, target_path: Path) -> bool:
        """复制文件"""
        try:
            shutil.copy2(str(source_path), str(target_path))
            self.logger.info(f"文件复制成功: {source_path} -> {target_path}")
            return True
        except Exception as e:
            self.logger.error(f"复制文件失败: {e}")
            return False

    def organize_file(
        self,
        media_info: Dict[str, Any],
        tmdb_data: Dict[str, Any],
        ai_data: Dict[str, Any],
        link_method: Optional[str] = None,
    ) -> Optional[Path]:
        """组织文件到媒体库"""
        is_anime = tmdb_data.get("is_anime", False)
        source_path = Path(media_info["file_path"])

        if tmdb_data["media_type"] == "movie":
            target_path = self._get_target_path(media_info, tmdb_data, is_anime)
        else:
            season = ai_data.get("season", 1)
            episode = ai_data.get("episode", 1)
            target_path = self._get_target_path(
                media_info, tmdb_data, is_anime, season, episode
            )

        method = link_method or self.link_method

        if self.create_link(source_path, target_path, method):
            return target_path
        return None
