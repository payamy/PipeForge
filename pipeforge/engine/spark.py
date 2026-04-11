from typing import List, Dict

from pyspark.sql.session import SparkSession
from delta import configure_spark_with_delta_pip


class __SingletonMeta(type):
    """
    Singleton metaclass for SparkWrapper class
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SparkWrapper(metaclass=__SingletonMeta):
    """
    A wrapper class for SparkSession. \
    There would be a single instance of SparkWrapper through the entire program
    """
    def __init__(self, dependencies: List[str] = None, master: str = 'local[*]',
                 app_name: str = 'replication_app', docker_image : str = 'spark:3.5.2-python3',
                 enable_delta: bool = False, warehouse_location: str = 's3a://hive/warehouse',
                 metastore_uri: str = 'thrift://localhost:9083',
                 spark_conf: Dict[str, str] = None):
        """
        :param dependencies: List of Maven dependencies in form of groupId:artifactId:version
        :param master: Spark master address. only local and k8s will be supported
        :param app_name: Spark app name
        :param docker_image: Spark docker image if the master is kubernetes
        :param enable_delta: Enable support for Delta tables
        """
        params = {
            "dependencies": ",".join(dependencies) if dependencies else None,
            "master": master,
            "app_name": app_name,
            "docker_image": docker_image
        }

        builder = SparkSession.builder.appName(app_name).master(params["master"])

        if params["master"].startswith("k8s"):
            builder = (builder
                .config("spark.kubernetes.container.image", params["docker_image"])
                .config("spark.submit.deployMode", "client")
            )
        elif params["master"].startswith("local"):
            pass
        else:
            raise ValueError("SparkWrapper only supports k8s or local \n"
                             "standalone and yarn are not supported.")
        # Apply any user-supplied Spark configs
        if spark_conf:
            for key, value in spark_conf.items():
                builder = builder.config(key, value)

        if enable_delta:
            if dependencies is None:
                dependencies = []
            builder = (builder
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.sql.warehouse.dir", warehouse_location)
                .config("hive.metastore.uris", metastore_uri)
                .enableHiveSupport()
            )
        elif params["dependencies"]:
            builder = builder.config("spark.jars.packages", params["dependencies"])

        self.__spark = builder.getOrCreate() if not enable_delta else \
            configure_spark_with_delta_pip(builder, extra_packages=dependencies).getOrCreate()

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @spark.getter
    def spark(self) -> SparkSession:
        return self.__spark

    def set_s3_configurations(self, endpoint: str, access_key: str, secret_key: str, ssl_enabled: bool = True):
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
