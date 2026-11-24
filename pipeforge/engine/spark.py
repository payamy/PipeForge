from __future__ import annotations

import logging
from typing import Dict, List
from urllib.parse import urlparse

from pyspark.sql.session import SparkSession

log = logging.getLogger("pipeforge")


class __SingletonMeta(type):
    """Singleton metaclass for SparkWrapper."""
    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    def reset(cls) -> None:
        """Drop the singleton (stops its session). Mainly for tests."""
        inst = cls._instances.pop(cls, None)
        if inst is not None:
            inst.spark.stop()


class SparkWrapper(metaclass=__SingletonMeta):
    """Single SparkSession for the process.

    Jars (Delta, hadoop-aws, AWS SDK, Postgres JDBC) are expected to already be
    on the classpath, as in the ``spark-base`` image. Nothing is downloaded
    unless ``packages`` is passed explicitly (local development only).
    """

    def __init__(self, dependencies: List[str] | None = None, master: str = "local[*]",
                 app_name: str = "replication_app", docker_image: str | None = None,
                 enable_delta: bool = False, warehouse_location: str | None = None,
                 metastore_uri: str | None = None, spark_conf: Dict[str, str] | None = None,
                 s3: Dict[str, object] | None = None, packages: List[str] | None = None):
        """
        :param dependencies: deprecated alias of ``packages``
        :param packages: Maven coordinates resolved at start-up. Opt-in, development only.
        :param s3: endpoint / access_key / secret_key / path_style / region settings for s3a
        :param metastore_uri: when set, Hive support is enabled against this thrift URI
        """
        if master.startswith("k8s"):
            if not docker_image:
                raise ValueError("docker_image is required for a k8s master")
        elif not master.startswith("local"):
            raise ValueError("SparkWrapper only supports k8s or local masters")

        builder = SparkSession.builder.appName(app_name).master(master)
        builder = builder.config("spark.sql.session.timeZone", "UTC")
        if master.startswith("k8s"):
            builder = (builder.config("spark.kubernetes.container.image", docker_image)
                       .config("spark.submit.deployMode", "client"))

        packages = packages or dependencies
        if packages:
            log.warning("resolving Maven packages at start-up: %s", packages)
            builder = builder.config("spark.jars.packages", ",".join(packages))

        for key, value in (spark_conf or {}).items():
            builder = builder.config(key, value)

        if s3:
            for key, value in self.s3_conf(**s3).items():
                builder = builder.config(key, value)

        if enable_delta:
            builder = (builder
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog",
                               "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        if warehouse_location:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_location)
        if metastore_uri:
            builder = builder.config("hive.metastore.uris", metastore_uri).enableHiveSupport()

        self.__spark = builder.getOrCreate()

    @staticmethod
    def s3_conf(endpoint: str | None = None, access_key: str | None = None,
                secret_key: str | None = None, path_style: bool = True,
                region: str | None = None, ssl_enabled: bool | None = None, **_ignored) -> Dict[str, str]:
        """Return spark.hadoop.fs.s3a.* settings (credentials are never logged)."""
        conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": str(bool(path_style)).lower(),
        }
        if endpoint:
            conf["spark.hadoop.fs.s3a.endpoint"] = endpoint
            if ssl_enabled is None:
                ssl_enabled = urlparse(endpoint).scheme != "http"
        if ssl_enabled is not None:
            conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(bool(ssl_enabled)).lower()
        if access_key:
            conf["spark.hadoop.fs.s3a.access.key"] = access_key
        if secret_key:
            conf["spark.hadoop.fs.s3a.secret.key"] = secret_key
        if region:
            conf["spark.hadoop.fs.s3a.endpoint.region"] = region
        return conf

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    def set_s3_configurations(self, endpoint: str, access_key: str, secret_key: str,
                              ssl_enabled: bool | None = None, path_style: bool = True) -> None:
        """Set s3a options on an already running session."""
        hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
        for key, value in self.s3_conf(endpoint, access_key, secret_key, path_style,
                                       ssl_enabled=ssl_enabled).items():
            hconf.set(key.replace("spark.hadoop.", "", 1), value)
