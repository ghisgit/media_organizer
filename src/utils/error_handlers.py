import logging
import time
import threading
from typing import Callable, Any, Type, Tuple
from functools import wraps
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """熔断器状态"""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """熔断器模式实现"""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.expected_exceptions = expected_exceptions

        self.failure_count = 0
        self.last_failure_time = 0
        self.state = CircuitState.CLOSED
        self._lock = threading.RLock()
        self._half_open_test_in_progress = False

        logger.debug(f"初始化熔断器: {name}")

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """通过熔断器调用函数"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.reset_timeout:
                    logger.info(f"熔断器转为半开状态: {self.name}")
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_test_in_progress = True
                else:
                    raise Exception(f"熔断器开启: {self.name}")

            elif (
                self.state == CircuitState.HALF_OPEN
                and self._half_open_test_in_progress
            ):
                raise Exception(f"熔断器测试中: {self.name}")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            if isinstance(e, self.expected_exceptions):
                self._on_failure(e)
            raise

    def _on_success(self) -> None:
        """处理成功请求"""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                logger.info(f"熔断器转为关闭状态: {self.name}")
                self.state = CircuitState.CLOSED
                self._half_open_test_in_progress = False

            self.failure_count = 0

    def _on_failure(self, error: Exception) -> None:
        """处理失败请求"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                logger.warning(f"熔断器测试失败: {self.name}")
                self.state = CircuitState.OPEN
                self._half_open_test_in_progress = False
            elif (
                self.state == CircuitState.CLOSED
                and self.failure_count >= self.failure_threshold
            ):
                logger.warning(f"熔断器转为开启状态: {self.name}")
                self.state = CircuitState.OPEN

    def get_status(self) -> dict:
        """获取熔断器状态"""
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time,
            }


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    max_delay: float = 60.0,
    expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """带指数退避的重试装饰器"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except expected_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.warning(f"函数重试后仍然失败: {func.__name__}")
                        break

                    current_delay = min(delay, max_delay)
                    logger.warning(
                        f"函数重试: {func.__name__}, " f"{current_delay:.2f} 秒后重试"
                    )

                    time.sleep(current_delay)
                    delay *= exponential_base

            raise last_exception

        return wrapper

    return decorator


class ResourceManager:
    """资源管理器"""

    def __init__(self):
        self._resources = []
        self._lock = threading.Lock()

    def register(self, resource, cleanup_func: Callable):
        """注册资源及其清理函数"""
        with self._lock:
            self._resources.append((resource, cleanup_func))

    def cleanup_all(self):
        """清理所有资源"""
        with self._lock:
            errors = []
            for resource, cleanup_func in self._resources:
                try:
                    cleanup_func(resource)
                    logger.debug(f"成功清理资源: {resource}")
                except Exception as e:
                    errors.append(f"清理资源失败: {e}")

            self._resources.clear()

            if errors:
                raise Exception(f"资源清理错误: {'; '.join(errors)}")
