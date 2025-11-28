import sqlite3
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging
import threading
import contextlib
import queue

logger = logging.getLogger(__name__)


class ThreadSafeDatabaseConnectionPool:
    """线程安全的数据库连接池"""

    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connection_pool = queue.Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()

        # 预创建连接
        for _ in range(min(2, max_connections)):
            self._create_and_add_connection()

    def _create_connection(self) -> sqlite3.Connection:
        """创建新的数据库连接"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # 优化设置
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-64000")

        return conn

    def _create_and_add_connection(self):
        """创建并添加连接到池中"""
        try:
            conn = self._create_connection()
            self._connection_pool.put(conn, block=False)
            with self._lock:
                self._created_connections += 1
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")

    @contextlib.contextmanager
    def get_connection(self):
        """获取数据库连接（上下文管理器）"""
        conn = None
        try:
            try:
                conn = self._connection_pool.get(timeout=10.0)
            except queue.Empty:
                with self._lock:
                    if self._created_connections < self.max_connections:
                        conn = self._create_connection()
                        self._created_connections += 1
                    else:
                        conn = self._connection_pool.get(timeout=30.0)

            yield conn
        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise
        finally:
            if conn:
                try:
                    self._connection_pool.put(conn, block=False)
                except queue.Full:
                    conn.close()

    def close_all(self):
        """关闭所有连接"""
        while not self._connection_pool.empty():
            try:
                conn = self._connection_pool.get_nowait()
                conn.close()
            except (queue.Empty, sqlite3.Error):
                pass


class DatabaseManager:
    """数据库管理基类"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logger
        self.connection_pool = ThreadSafeDatabaseConnectionPool(db_path)

    def execute_query(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """执行SQL查询"""
        with self.connection_pool.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return cursor
            except sqlite3.Error as e:
                conn.rollback()
                self.logger.error(f"数据库查询失败: {e}, 查询: {query}")
                raise


class TMDBCacheDB(DatabaseManager):
    """TMDB缓存数据库管理 - 修复缓存数据结构"""

    def __init__(self, db_path: str, expire_days: int = 30):
        super().__init__(db_path)
        self.expire_days = expire_days
        self.create_tables()

    def create_tables(self) -> None:
        """创建TMDB缓存表 - 添加 genre_ids 字段"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS tmdb_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_type TEXT NOT NULL,
                query_text TEXT NOT NULL,
                query_year INTEGER,
                tmdb_id INTEGER NOT NULL,
                media_type TEXT NOT NULL,
                title TEXT NOT NULL,
                release_year INTEGER,
                genres TEXT,
                genre_ids TEXT,  -- 新增字段：存储分类ID列表
                data_json TEXT NOT NULL,
                created_time INTEGER NOT NULL,
                last_accessed_time INTEGER NOT NULL,
                UNIQUE(query_type, query_text, query_year)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_query ON tmdb_cache(query_type, query_text, query_year)",
            "CREATE INDEX IF NOT EXISTS idx_access_time ON tmdb_cache(last_accessed_time)",
            "CREATE INDEX IF NOT EXISTS idx_tmdb_id ON tmdb_cache(tmdb_id)",
        ]

        for query in queries:
            try:
                self.execute_query(query)
            except Exception as e:
                self.logger.error(f"创建表失败: {e}")

        self._migrate_table_structure()
        self.logger.info("TMDB缓存表创建完成")

    def get_cache(
        self, query_type: str, query_text: str, year: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """获取缓存 - 修复返回数据结构"""
        if year is not None:
            query = """
            SELECT data_json, tmdb_id, media_type, title, release_year, genres, genre_ids 
            FROM tmdb_cache 
            WHERE query_type = ? AND query_text = ? AND query_year = ?
            """
            params = (query_type, query_text, year)
        else:
            query = """
            SELECT data_json, tmdb_id, media_type, title, release_year, genres, genre_ids 
            FROM tmdb_cache 
            WHERE query_type = ? AND query_text = ?
            """
            params = (query_type, query_text)

        try:
            cursor = self.execute_query(query, params)
            result = cursor.fetchone()

            if result:
                update_query = """
                UPDATE tmdb_cache SET last_accessed_time = ? 
                WHERE query_type = ? AND query_text = ? AND query_year = ?
                """
                self.execute_query(
                    update_query, (int(time.time()), query_type, query_text, year)
                )

                result_dict = dict(result)

                # 解析 genre_ids
                genre_ids = []
                if result_dict["genre_ids"]:
                    try:
                        genre_ids = json.loads(result_dict["genre_ids"])
                    except (json.JSONDecodeError, TypeError):
                        genre_ids = []

                # 构建完整的返回数据
                return {
                    "data": json.loads(result_dict["data_json"]),
                    "tmdb_id": result_dict["tmdb_id"],
                    "media_type": result_dict["media_type"],
                    "title": result_dict["title"],
                    "release_year": result_dict["release_year"],
                    "genres": (
                        json.loads(result_dict["genres"])
                        if result_dict["genres"]
                        else []
                    ),
                    "genre_ids": genre_ids,
                    "is_anime": 16 in genre_ids,  # 在缓存中直接判断是否为动漫
                }
            return None
        except Exception as e:
            self.logger.error(f"获取缓存失败: {e}")
            return None

    def set_cache(
        self,
        query_type: str,
        query_text: str,
        year: Optional[int],
        tmdb_id: int,
        media_type: str,
        title: str,
        release_year: int,
        genres: List[str],
        data: Dict[str, Any],
    ) -> None:
        """设置缓存 - 保存 genre_ids"""
        current_time = int(time.time())

        # 从原始数据中提取 genre_ids
        genre_ids = []
        if data and "genres" in data:
            genre_ids = [
                genre.get("id") for genre in data.get("genres", []) if genre.get("id")
            ]

        query = """
        INSERT OR REPLACE INTO tmdb_cache 
        (query_type, query_text, query_year, tmdb_id, media_type, title, release_year, genres, genre_ids, data_json, created_time, last_accessed_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            self.execute_query(
                query,
                (
                    query_type,
                    query_text,
                    year,
                    tmdb_id,
                    media_type,
                    title,
                    release_year,
                    json.dumps(genres),
                    json.dumps(genre_ids),  # 保存 genre_ids
                    json.dumps(data),
                    current_time,
                    current_time,
                ),
            )
            self.logger.debug(
                f"缓存设置成功: {query_type}/{query_text}/{year}, 动漫: {16 in genre_ids}"
            )
        except Exception as e:
            self.logger.error(f"设置缓存失败: {e}")
            raise

    def cleanup_expired(self) -> int:
        """清理过期缓存"""
        expire_time = int(time.time()) - (self.expire_days * 24 * 60 * 60)
        query = "DELETE FROM tmdb_cache WHERE last_accessed_time < ?"

        try:
            cursor = self.execute_query(query, (expire_time,))
            deleted_count = cursor.rowcount

            if deleted_count > 0:
                self.logger.info(f"清理了 {deleted_count} 个过期TMDB缓存记录")

            return deleted_count
        except Exception as e:
            self.logger.error(f"清理过期缓存失败: {e}")
            return 0

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        stats = {}

        try:
            cursor = self.execute_query("SELECT COUNT(*) FROM tmdb_cache")
            stats["total_cache_count"] = cursor.fetchone()[0]

            cursor = self.execute_query(
                "SELECT query_type, COUNT(*) FROM tmdb_cache GROUP BY query_type"
            )
            stats["cache_by_type"] = dict(cursor.fetchall())

            cursor = self.execute_query("SELECT SUM(LENGTH(data_json)) FROM tmdb_cache")
            total_size_bytes = cursor.fetchone()[0] or 0
            stats["total_cache_size_mb"] = round(total_size_bytes / (1024 * 1024), 2)

        except Exception as e:
            self.logger.error(f"获取缓存统计失败: {e}")

        return stats


class ProcessedFilesDB(DatabaseManager):
    """已处理文件数据库管理"""

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.create_tables()

    def create_tables(self) -> None:
        """创建已处理文件表"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_md5 TEXT,
                file_size INTEGER NOT NULL,
                processed_time INTEGER NOT NULL,
                tmdb_id INTEGER,
                media_type TEXT,
                target_path TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path)",
            "CREATE INDEX IF NOT EXISTS idx_md5 ON processed_files(file_md5)",
            "CREATE INDEX IF NOT EXISTS idx_processed_time ON processed_files(processed_time)",
            "CREATE INDEX IF NOT EXISTS idx_tmdb_id ON processed_files(tmdb_id)",
        ]

        for query in queries:
            try:
                self.execute_query(query)
            except Exception as e:
                self.logger.error(f"创建表失败: {e}")

        self._migrate_table_structure()
        self.logger.info("已处理文件表创建完成")

    def _migrate_table_structure(self) -> None:
        """迁移表结构"""
        try:
            cursor = self.execute_query("PRAGMA table_info(processed_files)")
            columns = cursor.fetchall()

            file_md5_column = next(
                (col for col in columns if col["name"] == "file_md5"), None
            )

            if file_md5_column and file_md5_column["notnull"] == 1:
                self.logger.info("检测到file_md5字段有NOT NULL约束，正在进行迁移...")

                self.execute_query(
                    """
                CREATE TABLE IF NOT EXISTS processed_files_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE NOT NULL,
                    file_md5 TEXT,
                    file_size INTEGER NOT NULL,
                    processed_time INTEGER NOT NULL,
                    tmdb_id INTEGER,
                    media_type TEXT,
                    target_path TEXT
                )
                """
                )

                self.execute_query(
                    """
                INSERT INTO processed_files_new 
                SELECT id, file_path, file_md5, file_size, processed_time, tmdb_id, media_type, target_path
                FROM processed_files
                """
                )

                self.execute_query("DROP TABLE processed_files")
                self.execute_query(
                    "ALTER TABLE processed_files_new RENAME TO processed_files"
                )

                # 重新创建索引
                for index_query in [
                    "CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path)",
                    "CREATE INDEX IF NOT EXISTS idx_md5 ON processed_files(file_md5)",
                    "CREATE INDEX IF NOT EXISTS idx_processed_time ON processed_files(processed_time)",
                    "CREATE INDEX IF NOT EXISTS idx_tmdb_id ON processed_files(tmdb_id)",
                ]:
                    self.execute_query(index_query)

                self.logger.info("表结构迁移完成")

        except Exception as e:
            self.logger.debug(f"表结构迁移检查完成: {e}")

    def is_processed_by_path_only(self, file_path: str) -> bool:
        """仅通过文件路径检查是否已处理（不检查MD5）"""
        query = "SELECT 1 FROM processed_files WHERE file_path = ?"

        try:
            cursor = self.execute_query(query, (file_path,))
            result = cursor.fetchone() is not None
            if result:
                self.logger.debug(f"文件路径已处理: {file_path}")
            return result
        except Exception as e:
            self.logger.error(f"检查文件路径是否已处理失败: {e}")
            return False

    def is_processed(
        self, file_path: str, md5: Optional[str] = None, use_md5: bool = True
    ) -> bool:
        """检查文件是否已处理"""
        if use_md5 and md5:
            query = "SELECT 1 FROM processed_files WHERE file_path = ? AND file_md5 = ?"
            params = (file_path, md5)
        else:
            query = "SELECT 1 FROM processed_files WHERE file_path = ?"
            params = (file_path,)

        try:
            cursor = self.execute_query(query, params)
            result = cursor.fetchone() is not None
            self.logger.debug(
                f"文件检查结果: {file_path} -> {'已处理' if result else '未处理'}"
            )
            return result
        except Exception as e:
            self.logger.error(f"检查文件是否已处理失败: {e}")
            return False

    def add_processed_file(
        self,
        file_path: str,
        file_size: int,
        md5: Optional[str] = None,
        tmdb_id: Optional[int] = None,
        media_type: Optional[str] = None,
        target_path: Optional[str] = None,
        use_md5: bool = True,
    ) -> None:
        """添加已处理文件记录"""
        if use_md5 and md5:
            query = """
            INSERT OR REPLACE INTO processed_files 
            (file_path, file_md5, file_size, processed_time, tmdb_id, media_type, target_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                file_path,
                md5,
                file_size,
                int(time.time()),
                tmdb_id,
                media_type,
                target_path,
            )
        else:
            query = """
            INSERT OR REPLACE INTO processed_files 
            (file_path, file_md5, file_size, processed_time, tmdb_id, media_type, target_path)
            VALUES (?, NULL, ?, ?, ?, ?, ?)
            """
            params = (
                file_path,
                file_size,
                int(time.time()),
                tmdb_id,
                media_type,
                target_path,
            )

        try:
            self.execute_query(query, params)
            self.logger.debug(f"已处理文件记录添加成功: {file_path}")
        except Exception as e:
            self.logger.error(f"添加已处理文件记录失败: {e}")
            raise

    def get_processed_count(self) -> int:
        """获取已处理文件数量"""
        query = "SELECT COUNT(*) FROM processed_files"

        try:
            cursor = self.execute_query(query)
            return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"获取已处理文件数量失败: {e}")
            return 0

    def get_recently_processed(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取最近处理的文件"""
        query = """
        SELECT file_path, file_md5, file_size, processed_time, tmdb_id, media_type, target_path
        FROM processed_files 
        ORDER BY processed_time DESC 
        LIMIT ?
        """

        try:
            cursor = self.execute_query(query, (limit,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"获取最近处理的文件失败: {e}")
            return []

    def cleanup_old_records(self, days: int = 30) -> int:
        """清理旧记录"""
        cutoff_time = int(time.time()) - (days * 24 * 60 * 60)
        query = "DELETE FROM processed_files WHERE processed_time < ?"

        try:
            cursor = self.execute_query(query, (cutoff_time,))
            deleted_count = cursor.rowcount

            if deleted_count > 0:
                self.logger.info(f"清理了 {deleted_count} 个 {days} 天前的记录")

            return deleted_count
        except Exception as e:
            self.logger.error(f"清理旧记录失败: {e}")
            return 0

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats = {}

        try:
            stats["processed_files_count"] = self.get_processed_count()

            cursor = self.execute_query(
                "SELECT media_type, COUNT(*) FROM processed_files GROUP BY media_type"
            )
            stats["files_by_media_type"] = dict(cursor.fetchall())

            db_size = Path(self.db_path).stat().st_size
            stats["database_size_mb"] = round(db_size / (1024 * 1024), 2)

            day_ago = int(time.time()) - 86400
            cursor = self.execute_query(
                "SELECT COUNT(*) FROM processed_files WHERE processed_time > ?",
                (day_ago,),
            )
            stats["files_processed_last_24h"] = cursor.fetchone()[0]

        except Exception as e:
            self.logger.error(f"获取数据库统计失败: {e}")

        return stats
