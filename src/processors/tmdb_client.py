import logging
from typing import Optional, Dict, Any, List
import tmdbsimple as tmdb
from ..core.database import TMDBCacheDB


class TMDBClient:
    """TMDB客户端 - 使用分类ID判断动漫"""

    def __init__(self, api_key: str, cache_db: TMDBCacheDB, proxy: str = ""):
        self.api_key = api_key
        self.cache_db = cache_db
        self.logger = logging.getLogger(__name__)

        # 配置tmdbsimple
        tmdb.API_KEY = api_key
        tmdb.REQUESTS_TIMEOUT = 10
        self.language = "zh-CN"

        # 设置代理
        if proxy:
            import requests

            session = requests.Session()
            session.proxies = {"http": proxy, "https": proxy}
            tmdb.REQUESTS_SESSION = session

        self._test_connection()
        self.logger.info("TMDB客户端初始化完成")

    def _test_connection(self):
        """测试连接"""
        try:
            config = tmdb.Configuration()
            response = config.info()
            if "images" in response:
                self.logger.info("TMDB连接测试成功")
            else:
                raise Exception("TMDB返回异常响应")
        except Exception as e:
            self.logger.error(f"TMDB连接测试失败: {e}")
            if "401" in str(e):
                raise Exception("TMDB认证失败：请检查API密钥")
            raise

    def search_movie(
        self, title: str, year: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """搜索电影"""
        # 检查缓存
        cached = self.cache_db.get_cache("movie", title, year)
        if cached:
            self.logger.debug(f"使用缓存: {title}")
            return cached

        try:
            search = tmdb.Search()
            params = {"query": title}
            if year:
                params["primary_release_year"] = year

            response = search.movie(**params)

            if search.results:
                return self._process_movie_result(search.results[0], title, year)

            self.logger.warning(f"未找到电影: {title}")
            return None

        except Exception as e:
            self.logger.error(f"搜索电影失败: {e}")
            return None

    def search_tv(self, title: str) -> Optional[Dict[str, Any]]:
        """搜索电视剧"""
        cached = self.cache_db.get_cache("tv", title, None)
        if cached:
            self.logger.debug(f"使用缓存: {title}")
            return cached

        try:
            search = tmdb.Search()
            response = search.tv(query=title)

            if search.results:
                return self._process_tv_result(search.results[0], title)

            self.logger.warning(f"未找到电视剧: {title}")
            return None

        except Exception as e:
            self.logger.error(f"搜索电视剧失败: {e}")
            return None

    def _process_movie_result(
        self, result: Dict, title: str, year: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """处理电影搜索结果"""
        movie_id = result["id"]
        details = self.get_movie_details(movie_id)

        if not details:
            return None

        release_year = self._extract_year(details.get("release_date"))
        genres = [genre["name"] for genre in details.get("genres", [])]
        genre_ids = [genre["id"] for genre in details.get("genres", [])]

        # 构建结果
        result_data = {
            "data": details,
            "tmdb_id": movie_id,
            "media_type": "movie",
            "title": details["title"],
            "release_year": release_year,
            "genres": genres,
            "genre_ids": genre_ids,
            "is_anime": self.is_anime_by_genre_ids(genre_ids),  # 使用分类ID判断
        }

        # 缓存结果
        self.cache_db.set_cache(
            "movie",
            title,
            year,
            movie_id,
            "movie",
            details["title"],
            release_year,
            genres,
            details,
        )

        self.logger.debug(
            f"电影搜索成功: {title} -> {details['title']}, 动漫: {result_data['is_anime']}"
        )
        return result_data

    def _process_tv_result(self, result: Dict, title: str) -> Optional[Dict[str, Any]]:
        """处理电视剧搜索结果"""
        tv_id = result["id"]
        details = self.get_tv_details(tv_id)

        if not details:
            return None

        release_year = self._extract_year(details.get("first_air_date"))
        genres = [genre["name"] for genre in details.get("genres", [])]
        genre_ids = [genre["id"] for genre in details.get("genres", [])]

        # 构建结果
        result_data = {
            "data": details,
            "tmdb_id": tv_id,
            "media_type": "tv",
            "title": details["name"],
            "release_year": release_year,
            "genres": genres,
            "genre_ids": genre_ids,
            "is_anime": self.is_anime_by_genre_ids(genre_ids),  # 使用分类ID判断
        }

        # 缓存结果
        self.cache_db.set_cache(
            "tv",
            title,
            None,
            tv_id,
            "tv",
            details["name"],
            release_year,
            genres,
            details,
        )

        self.logger.debug(
            f"电视剧搜索成功: {title} -> {details['name']}, 动漫: {result_data['is_anime']}"
        )
        return result_data

    def _extract_year(self, date_str: Optional[str]) -> Optional[int]:
        """从日期字符串提取年份"""
        if date_str and len(date_str) >= 4:
            try:
                return int(date_str[:4])
            except (ValueError, TypeError):
                pass
        return None

    def get_movie_details(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """获取电影详情"""
        try:
            movie = tmdb.Movies(movie_id)
            return movie.info(language=self.language)
        except Exception as e:
            self.logger.error(f"获取电影详情失败 {movie_id}: {e}")
            return None

    def get_tv_details(self, tv_id: int) -> Optional[Dict[str, Any]]:
        """获取电视剧详情"""
        try:
            tv = tmdb.TV(tv_id)
            return tv.info(language=self.language)
        except Exception as e:
            self.logger.error(f"获取电视剧详情失败 {tv_id}: {e}")
            return None

    def is_anime_by_genre_ids(self, genre_ids: List[int]) -> bool:
        """
        通过分类ID判断是否为动漫
        TMDB分类ID 16 = Animation
        """
        return 16 in genre_ids

    def get_configuration(self) -> Optional[Dict[str, Any]]:
        """获取配置信息"""
        try:
            config = tmdb.Configuration()
            return config.info()
        except Exception as e:
            self.logger.error(f"获取TMDB配置失败: {e}")
            return None

    def get_client_info(self) -> Dict[str, Any]:
        """获取客户端信息"""
        return {
            "library": "tmdbsimple",
            "version": getattr(tmdb, "__version__", "unknown"),
            "language": self.language,
        }
