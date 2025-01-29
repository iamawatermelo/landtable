"""
Landtable configuration
"""

from pydantic import BaseModel


class ConfigurationModel(BaseModel):
    cache_expiry_time: int = 3600
    port: int = 7228
    etcd_hostname: str = "localhost"
    etcd_port: int = 2379
    etcd_username: str | None = None
    etcd_password: str | None = None
