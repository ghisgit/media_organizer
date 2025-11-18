import time
import threading
import psutil
import requests
from typing import Callable, Dict, Any, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class HealthCheck:
    """健康检查基类"""

    def __init__(self, name: str):
        self.name = name

    def check(self) -> Dict[str, Any]:
        """执行健康检查"""
        raise NotImplementedError


class DatabaseHealthCheck(HealthCheck):
    """数据库健康检查"""

    def __init__(self, database_manager):
        super().__init__("database")
        self.database_manager = database_manager

    def check(self) -> Dict[str, Any]:
        """检查数据库健康状态"""
        try:
            # 简单的查询测试
            start_time = time.time()
            cursor = self.database_manager.execute_query("SELECT 1")
            result = cursor.fetchone()
            query_time = time.time() - start_time

            return {
                "status": "healthy",
                "query_time_seconds": round(query_time, 4),
                "test_result": result[0] == 1 if result else False,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


class FilesystemHealthCheck(HealthCheck):
    """文件系统健康检查"""

    def __init__(self, monitor_directories: List[Path], library_path: Path):
        super().__init__("filesystem")
        self.monitor_directories = monitor_directories
        self.library_path = library_path

    def check(self) -> Dict[str, Any]:
        """检查文件系统健康状态"""
        checks = {}
        all_healthy = True

        # 检查监控目录
        for i, directory in enumerate(self.monitor_directories):
            check_key = f"monitor_dir_{i}"
            try:
                if not directory.exists():
                    checks[check_key] = {
                        "status": "unhealthy",
                        "error": f"目录不存在: {directory}",
                        "path": str(directory),
                    }
                    all_healthy = False
                elif not directory.is_dir():
                    checks[check_key] = {
                        "status": "unhealthy",
                        "error": f"不是目录: {directory}",
                        "path": str(directory),
                    }
                    all_healthy = False
                else:
                    # 检查读取权限
                    try:
                        test_file = directory / ".health_check_test"
                        test_file.touch(exist_ok=True)
                        test_file.unlink()
                        checks[check_key] = {
                            "status": "healthy",
                            "permissions": "read_write",
                            "path": str(directory),
                        }
                    except PermissionError:
                        checks[check_key] = {
                            "status": "healthy",
                            "permissions": "read_only",
                            "path": str(directory),
                        }
                    except Exception as e:
                        checks[check_key] = {
                            "status": "unhealthy",
                            "error": f"权限检查失败: {e}",
                            "path": str(directory),
                        }
                        all_healthy = False
            except Exception as e:
                checks[check_key] = {
                    "status": "unhealthy",
                    "error": f"检查目录时发生错误: {e}",
                    "path": str(directory),
                }
                all_healthy = False

        # 检查媒体库目录
        try:
            if not self.library_path.exists():
                # 尝试创建目录
                try:
                    self.library_path.mkdir(parents=True, exist_ok=True)
                    checks["library"] = {
                        "status": "healthy",
                        "permissions": "read_write",
                        "created": True,
                        "path": str(self.library_path),
                    }
                    logger.info(f"健康检查创建了媒体库目录: {self.library_path}")
                except Exception as e:
                    checks["library"] = {
                        "status": "unhealthy",
                        "error": f"目录不存在且无法创建: {e}",
                        "path": str(self.library_path),
                    }
                    all_healthy = False
            else:
                # 检查是否是目录
                if not self.library_path.is_dir():
                    checks["library"] = {
                        "status": "unhealthy",
                        "error": "媒体库路径不是目录",
                        "path": str(self.library_path),
                    }
                    all_healthy = False
                else:
                    # 检查写入权限 - 直接测试媒体库目录本身
                    try:
                        test_dir = self.library_path / ".health_check_test"
                        test_dir.mkdir(exist_ok=True)
                        test_file = test_dir / "test.txt"
                        test_file.write_text("health_check")
                        test_file.unlink()
                        test_dir.rmdir()
                        checks["library"] = {
                            "status": "healthy",
                            "permissions": "read_write",
                            "path": str(self.library_path),
                        }
                    except PermissionError:
                        checks["library"] = {
                            "status": "unhealthy",
                            "error": "媒体库目录无写入权限",
                            "path": str(self.library_path),
                        }
                        all_healthy = False
                    except Exception as e:
                        checks["library"] = {
                            "status": "unhealthy",
                            "error": f"写入测试失败: {e}",
                            "path": str(self.library_path),
                        }
                        all_healthy = False
        except Exception as e:
            checks["library"] = {
                "status": "unhealthy",
                "error": f"媒体库目录检查失败: {e}",
                "path": str(self.library_path),
            }
            all_healthy = False

        return {"status": "healthy" if all_healthy else "unhealthy", "details": checks}


class SystemResourcesHealthCheck(HealthCheck):
    """系统资源健康检查"""

    def __init__(self):
        super().__init__("system_resources")

    def check(self) -> Dict[str, Any]:
        """检查系统资源状态"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)

            # 内存使用率
            memory = psutil.virtual_memory()

            # 磁盘使用率（使用第一个分区）
            disk_usage = psutil.disk_usage("/")

            return {
                "status": "healthy",
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": round(memory.available / (1024**3), 2),
                "disk_percent": disk_usage.percent,
                "disk_free_gb": round(disk_usage.free / (1024**3), 2),
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


class APIHealthCheck(HealthCheck):
    """API健康检查 - 修复版本"""

    def __init__(self, tmdb_client, ai_processor, config):
        super().__init__("apis")
        self.tmdb_client = tmdb_client
        self.ai_processor = ai_processor
        self.config = config

    def check(self) -> Dict[str, Any]:
        """检查API健康状态 - 修复版本"""
        checks = {}
        all_healthy = True

        # 检查TMDB API配置
        try:
            if (
                not self.tmdb_client.api_key
                or self.tmdb_client.api_key == "your_tmdb_api_key"
            ):
                checks["tmdb"] = {
                    "status": "unconfigured",
                    "api_key_set": False,
                    "error": "TMDB API密钥未配置，请在config.ini中设置tmdb_api_key",
                }
                all_healthy = False
            else:
                # 使用 tmdbsimple 测试连接
                try:
                    config = self.tmdb_client.get_configuration()
                    if config and "images" in config:
                        client_info = self.tmdb_client.get_client_info()
                        checks["tmdb"] = {
                            "status": "healthy",
                            "api_key_set": True,
                            "library": client_info["library"],
                            "version": client_info["version"],
                            "message": "TMDB API连接正常（使用tmdbsimple）",
                        }
                    else:
                        checks["tmdb"] = {
                            "status": "unhealthy",
                            "api_key_set": True,
                            "error": "TMDB API返回异常响应",
                        }
                        all_healthy = False
                except Exception as e:
                    if "401" in str(e):
                        checks["tmdb"] = {
                            "status": "unhealthy",
                            "api_key_set": True,
                            "error": "TMDB API密钥无效或已过期",
                        }
                    else:
                        checks["tmdb"] = {
                            "status": "unhealthy",
                            "api_key_set": True,
                            "error": f"TMDB API连接测试失败: {e}",
                        }
                    all_healthy = False
        except Exception as e:
            checks["tmdb"] = {"status": "error", "error": f"TMDB配置检查失败: {e}"}
            all_healthy = False

        # 检查AI服务配置 - 修复版本
        try:
            # 使用修复后的 get_ai_status 方法
            ai_status = self.ai_processor.get_ai_status()

            if self.config.ai_type == "deepseek":
                if (
                    not self.config.deepseek_api_key
                    or self.config.deepseek_api_key == "your_deepseek_api_key"
                ):
                    checks["ai"] = {
                        "status": "unconfigured",
                        "type": "deepseek",
                        "error": "DeepSeek API密钥未配置，请在config.ini中设置deepseek_api_key",
                    }
                    all_healthy = False
                else:
                    checks["ai"] = {
                        "status": "configured",
                        "type": "deepseek",
                        "configured": ai_status.get("configured", False),
                        "max_concurrent": ai_status.get("max_concurrent", 0),
                        "available_services": ai_status.get("available_services", []),
                        "message": (
                            "DeepSeek API已配置"
                            if ai_status.get("configured")
                            else "DeepSeek API配置异常"
                        ),
                    }

            elif self.config.ai_type == "spark":
                if (
                    not self.config.spark_api_key
                    or self.config.spark_api_key == "your_spark_api_key"
                ):
                    checks["ai"] = {
                        "status": "unconfigured",
                        "type": "spark",
                        "error": "讯飞星火API密钥未配置，请在config.ini中设置spark_api_key",
                    }
                    all_healthy = False
                else:
                    checks["ai"] = {
                        "status": "configured",
                        "type": "spark",
                        "configured": ai_status.get("configured", False),
                        "max_concurrent": ai_status.get("max_concurrent", 0),
                        "available_services": ai_status.get("available_services", []),
                        "message": (
                            "讯飞星火API已配置"
                            if ai_status.get("configured")
                            else "讯飞星火API配置异常"
                        ),
                    }

            elif self.config.ai_type == "model_scope":
                if (
                    not self.config.model_scope_api_key
                    or self.config.model_scope_api_key == "your_model_scope_api_key"
                ):
                    checks["ai"] = {
                        "status": "unconfigured",
                        "type": "model_scope",
                        "error": "魔塔API-Inference密钥未配置，请在config.ini中设置model_scope_api_key",
                    }
                    all_healthy = False
                else:
                    checks["ai"] = {
                        "status": "configured",
                        "type": "model_scope",
                        "configured": ai_status.get("configured", False),
                        "max_concurrent": ai_status.get("max_concurrent", 0),
                        "available_services": ai_status.get("available_services", []),
                        "message": (
                            "魔塔API-Inference已配置"
                            if ai_status.get("configured")
                            else "魔塔API-Inference配置异常"
                        ),
                    }

            elif self.config.ai_type == "zhipu":
                if (
                    not self.config.zhipu_api_key
                    or self.config.zhipu_api_key == "your_zhipu_api_key"
                ):
                    checks["ai"] = {
                        "status": "unconfigured",
                        "type": "zhipu",
                        "error": "智普AI密钥未配置，请在config.ini中设置zhipu_api_key",
                    }
                    all_healthy = False
                else:
                    checks["ai"] = {
                        "status": "configured",
                        "type": "zhipu",
                        "configured": ai_status.get("configured", False),
                        "max_concurrent": ai_status.get("max_concurrent", 0),
                        "available_services": ai_status.get("available_services", []),
                        "message": (
                            "智普AI已配置"
                            if ai_status.get("configured")
                            else "智普AI配置异常"
                        ),
                    }
            else:
                checks["ai"] = {
                    "status": "unknown",
                    "type": self.config.ai_type,
                    "error": f"不支持的AI类型: {self.config.ai_type}",
                }
                all_healthy = False

        except Exception as e:
            checks["ai"] = {"status": "error", "error": f"AI服务检查失败: {e}"}
            all_healthy = False

        return {"status": "healthy" if all_healthy else "unhealthy", "details": checks}


class HealthMonitor:
    """健康监控器"""

    def __init__(self, check_interval: int = 300):  # 5分钟
        self.check_interval = check_interval
        self.health_checks: Dict[str, HealthCheck] = {}
        self.last_results: Dict[str, Any] = {}
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        logger.info(f"初始化健康监控器，检查间隔: {check_interval}秒")

    def add_health_check(self, name: str, health_check: HealthCheck):
        """添加健康检查"""
        with self._lock:
            self.health_checks[name] = health_check
            logger.debug(f"添加健康检查: {name}")

    def start(self):
        """开始健康监控"""
        if self.running:
            logger.warning("健康监控器已经在运行")
            return

        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="HealthMonitor"
        )
        self.monitor_thread.start()
        logger.info("健康监控器已启动")

    def stop(self):
        """停止健康监控"""
        if not self.running:
            return

        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
            self.monitor_thread = None

        logger.info("健康监控器已停止")

    def _monitor_loop(self):
        """监控循环"""
        while self.running:
            try:
                current_results = {}

                # 执行所有健康检查
                for name, health_check in self.health_checks.items():
                    try:
                        result = health_check.check()
                        current_results[name] = result

                        # 记录警告状态
                        if result.get("status") == "unhealthy":
                            logger.warning(
                                f"健康检查 '{name}' 失败: {result.get('error', '未知错误')}"
                            )

                    except Exception as e:
                        current_results[name] = {
                            "status": "error",
                            "error": f"执行检查时发生错误: {e}",
                        }
                        logger.error(f"执行健康检查 '{name}' 时发生错误: {e}")

                # 更新结果
                with self._lock:
                    self.last_results = current_results

                # 记录周期性状态
                if any(
                    result.get("status") in ["unhealthy", "error"]
                    for result in current_results.values()
                ):
                    logger.warning("系统健康状态异常")
                else:
                    logger.debug("系统健康状态正常")

            except Exception as e:
                logger.error(f"健康监控循环发生错误: {e}")

            # 等待下一次检查
            for _ in range(self.check_interval):
                if not self.running:
                    break
                time.sleep(1)

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        with self._lock:
            return self.last_results.copy()

    def is_healthy(self) -> bool:
        """检查系统是否健康"""
        with self._lock:
            if not self.last_results:
                return False

            return all(
                result.get("status") == "healthy"
                for result in self.last_results.values()
            )

    def get_unhealthy_components(self) -> List[str]:
        """获取不健康的组件列表"""
        with self._lock:
            return [
                name
                for name, result in self.last_results.items()
                if result.get("status") != "healthy"
            ]
