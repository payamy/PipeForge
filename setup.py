import re
from pathlib import Path

from setuptools import setup, find_packages

ROOT = Path(__file__).parent
VERSION = re.search(r'__version__ = "([^"]+)"', (ROOT / "pipeforge" / "__init__.py").read_text()).group(1)

setup(
    name="pipeforge",
    version=VERSION,
    author="payamy",
    description="Utility for replicating data from sources to data warehouses",
    long_description=(ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    python_requires=">=3.10,<3.12",
    packages=find_packages(include=["pipeforge", "pipeforge.*"]),
    package_data={"pipeforge.spec": ["*.json"]},
    # pyspark and delta-spark are deliberately NOT here: images built on spark-base
    # already contain Spark. Install `pipeforge[spark]` on a bare Python.
    install_requires=[
        "psycopg2-binary==2.9.9",
        "pandas==2.2.1",
        "numpy==1.26.4",
        "hvac==2.0.0",
        "requests==2.32.3",
        "urllib3==2.2.3",
        "boto3==1.35.45",
        "botocore==1.35.45",
        "jsonschema==4.23.0",
    ],
    extras_require={
        "spark": ["pyspark==3.5.2", "delta-spark==3.2.0"],
        "dev": ["pytest==8.3.3", "ruff==0.7.1", "moto[s3]==5.0.18"],
    },
    entry_points={"console_scripts": ["pipeforge=pipeforge.cli:main"]},
)
