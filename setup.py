from setuptools import setup, find_packages
from typing import List
from pathlib import Path


def get_install_requires() -> List[str]:
    """Returns requirements.txt parsed to a list"""
    f_name = Path(__file__).parent / 'requirements.txt'
    targets = []
    if f_name.exists():
        with open(f_name, 'r') as f:
            targets = f.read().splitlines()
    return targets


setup(
    name='pipeforge',
    version='1.0.0',
    author='payamy',
    description='Utility for replicating data from sources to data warehouses',
    python_requires='>=3.11, <3.12',
    packages=find_packages(),  # Automatically find all packages and sub-packages
    install_requires=get_install_requires()
)
