import json
import logging
import threading
from typing import Optional, Dict, Any
from openai import OpenAI, OpenAIError


class AIProcessor:
    """AI处理器 - 支持多种AI服务"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化客户端
        self.clients = {}
        self._init_clients()

        # 并发控制
        self.semaphore = threading.Semaphore(self.config.ai_max_concurrent)

        # 统计信息
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "last_error": None,
        }

        self.logger.info(f"AI处理器初始化完成 - 服务: {list(self.clients.keys())}")

    def _init_clients(self):
        """初始化AI服务客户端"""
        services = {
            "deepseek": {
                "api_key": self.config.deepseek_api_key,
                "base_url": self.config.deepseek_url,
                "default_key": "your_deepseek_api_key",
            },
            "spark": {
                "api_key": self.config.spark_api_key,
                "base_url": self.config.spark_url,
                "default_key": "your_spark_api_key",
            },
            "model_scope": {
                "api_key": self.config.model_scope_api_key,
                "base_url": self.config.model_scope_url,
                "default_key": "your_model_scope_api_key",
            },
            "zhipu": {
                "api_key": self.config.zhipu_api_key,
                "base_url": self.config.zhipu_url,
                "default_key": "your_zhipu_api_key",
            },
        }

        for service, config in services.items():
            if config["api_key"] and config["api_key"] != config["default_key"]:
                self.clients[service] = OpenAI(
                    api_key=config["api_key"], base_url=config["base_url"]
                )
                self.logger.info(f"{service}客户端初始化完成")

    def extract_media_info(self, filename: str) -> Optional[Dict[str, Any]]:
        """提取媒体信息"""
        if self.config.ai_type not in self.clients:
            self.logger.error(f"AI服务未配置: {self.config.ai_type}")
            return None

        # 获取信号量
        acquired = self.semaphore.acquire(blocking=False)
        if not acquired:
            self.logger.warning("AI服务并发限制，跳过处理")
            return None

        try:
            self.stats["total_requests"] += 1
            result = self._extract_with_client(filename, self.config.ai_type)
            if result:
                self.stats["successful_requests"] += 1
            else:
                self.stats["failed_requests"] += 1
            return result
        except Exception as e:
            self.stats["failed_requests"] += 1
            self.stats["last_error"] = str(e)
            self.logger.error(f"AI处理失败: {e}")
            return None
        finally:
            self.semaphore.release()

    def _extract_with_client(
        self, filename: str, service_type: str
    ) -> Optional[Dict[str, Any]]:
        """使用指定客户端提取信息"""
        client = self.clients.get(service_type)
        if not client:
            return None

        prompt = self._build_prompt(filename)

        try:
            self.logger.debug(f"发送{service_type}请求: {filename}")

            model_params = self._get_model_params(service_type)
            response = client.chat.completions.create(
                model=model_params["model"],
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个媒体文件分析助手。请从文件名中提取电影或电视剧信息，并返回标准的JSON格式。",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=self.config.ai_max_tokens,
                **model_params.get("extra_params", {}),
            )

            content = response.choices[0].message.content
            return self._parse_ai_response(content)

        except OpenAIError as e:
            self.logger.error(f"{service_type} API请求失败: {e}")
            return None
        except Exception as e:
            self.logger.error(f"{service_type}处理失败: {e}")
            return None

    def _get_model_params(self, service_type: str) -> Dict[str, Any]:
        """获取模型参数"""
        params = {
            "deepseek": {
                "model": "deepseek-chat",
                "extra_params": {"response_format": {"type": "json_object"}},
            },
            "spark": {
                "model": self.config.spark_model,
                "extra_params": {"response_format": {"type": "json_object"}},
            },
            "model_scope": {
                "model": self.config.model_scope_model,
                "extra_params": {"response_format": {"type": "json_object"}},
            },
            "zhipu": {
                "model": self.config.zhipu_model,
                "extra_params": {
                    "response_format": {"type": "json_object"},
                    "extra_body": {
                        "do_sample": False,
                        "thinking": {"type": "disabled"},
                    },
                },
            },
        }
        return params.get(service_type, {"model": "default"})

    def _build_prompt(self, filename: str) -> str:
        """构建提示词"""
        return f"""分析这个文件名，告诉我这是电影还是电视剧：

文件名：{filename}

如果是电影，返回：{{"type": "movie", "title": "电影名称", "year": 年份}}
如果是电视剧，返回：{{"type": "tv", "title": "剧集名称", "season": 季数, "episode": 集数}}

注意：
- 年份、季数、集数都必须是数字
- 名称要简洁规范"""

    def _parse_ai_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析AI响应"""
        try:
            start = response.find("{")
            end = response.rfind("}") + 1
            if start == -1 or end == 0:
                return None

            json_str = response[start:end]
            data = json.loads(json_str)

            # 验证数据
            if data.get("type") not in ["movie", "tv"]:
                return None

            if data["type"] == "movie":
                if not data.get("title"):
                    return None
                if "year" in data and not isinstance(data["year"], int):
                    return None
            else:
                if not data.get("title"):
                    return None
                if data.get("season") is None or not isinstance(data["season"], int):
                    return None
                if data.get("episode") is None or not isinstance(data["episode"], int):
                    return None

            self.logger.info(f"AI解析成功: {data}")
            return data

        except (json.JSONDecodeError, Exception):
            return None

    def get_ai_status(self) -> Dict[str, Any]:
        """获取AI服务状态信息 - 修复缺失的方法"""
        try:
            configured = self.config.ai_type in self.clients

            if configured:
                client_config = {
                    "deepseek": {
                        "api_key_set": bool(
                            self.config.deepseek_api_key
                            and self.config.deepseek_api_key != "your_deepseek_api_key"
                        ),
                        "url": self.config.deepseek_url,
                    },
                    "spark": {
                        "api_key_set": bool(
                            self.config.spark_api_key
                            and self.config.spark_api_key != "your_spark_api_key"
                        ),
                        "url": self.config.spark_url,
                        "model": self.config.spark_model,
                    },
                    "model_scope": {
                        "api_key_set": bool(
                            self.config.model_scope_api_key
                            and self.config.model_scope_api_key
                            != "your_model_scope_api_key"
                        ),
                        "url": self.config.model_scope_url,
                        "model": self.config.model_scope_model,
                    },
                    "zhipu": {
                        "api_key_set": bool(
                            self.config.zhipu_api_key
                            and self.config.zhipu_api_key != "your_zhipu_api_key"
                        ),
                        "url": self.config.zhipu_url,
                        "model": self.config.zhipu_model,
                    },
                }

                current_config = client_config.get(self.config.ai_type, {})
            else:
                current_config = {}

            return {
                "ai_type": self.config.ai_type,
                "configured": configured,
                "current_config": current_config,
                "max_concurrent": self.config.ai_max_concurrent,
                "max_tokens": self.config.ai_max_tokens,
                "available_services": list(self.clients.keys()),
                "stats": self.stats,
                "concurrent_available": self.semaphore._value,
                "limit_type": "concurrent_limit",
            }
        except Exception as e:
            self.logger.error(f"获取AI状态失败: {e}")
            return {
                "ai_type": self.config.ai_type,
                "configured": False,
                "error": str(e),
            }

    def get_status(self) -> Dict[str, Any]:
        """获取状态信息 - 兼容性方法"""
        return self.get_ai_status()
