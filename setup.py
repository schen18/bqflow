"""
setup.py for bqflow package

Usage:
    python setup.py install
    
Or with pip:
    pip install -e .
    
To build distribution:
    python setup.py sdist bdist_wheel
"""

from setuptools import setup, find_packages
import os

# Read version from __version__.py
version = {}
with open(os.path.join('bqflow', '__version__.py')) as f:
    exec(f.read(), version)

# Read long description from README
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

# Read requirements
with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='bqflow',
    version=version['__version__'],
    author=version['__author__'],
    author_email=version['__email__'],
    description=version['__description__'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=version['__url__'],
    license=version['__license__'],
    packages=find_packages(exclude=['tests', 'tests.*', 'examples', 'docs']),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='bigquery gcs parquet sql export google-cloud',
    python_requires='>=3.8',
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'mypy>=1.0.0',
        ],
        'pandas': ['pandas>=2.0.0'],
        'polars': ['polars>=0.19.0'],
        'spark': ['pyspark>=3.4.0'],
        'arrow': ['pyarrow>=12.0.0'],
        'all': [
            'pandas>=2.0.0',
            'polars>=0.19.0',
            'pyspark>=3.4.0',
            'pyarrow>=12.0.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'bq-export=bqflow.cli:main',
        ],
    },
    project_urls={
        'Bug Reports': f"{version['__url__']}/issues",
        'Source': version['__url__'],
        'Documentation': f"{version['__url__']}/blob/main/README.md",
    },
)