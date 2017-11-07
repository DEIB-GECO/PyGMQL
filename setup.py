from setuptools import setup, find_packages
import os
import sys


with open(os.path.join(".", "gmql", "resources", "version"), "r") as f_ver:
    version = f_ver.read().strip()

with open(os.path.join(".", "gmql", "resources", "github_url"), "r") as f_ver:
    github_url = f_ver.read().strip()

# Checking the numpy installation
try:
    import numpy
except ImportError:
    sys.exit("install requires: 'numpy'.\n"
             "Use 'pip install numpy' first")


setup(name='gmql',
      version=version,
      description='Python library for GMQL',
      long_description="PyGMQL is a python module that enables the user "
                       "to perform operation on genomic data in a scalable way.",
      url=github_url,
      author='Luca Nanni, Mustafa Anil Tuncel',
      author_email='lucananni93dev@gmail.com',
      maintainer="Mustafa Anil Tuncel",
      maintainer_email="tuncel.manil@gmail.com",
      license='MIT',
      download_url='{}/tarball/{}'.format(github_url, version),
      packages=find_packages(),
      install_requires=['pandas', 'tqdm', 'numpy', 'py4j',
                        'psutil', 'requests', 'requests-toolbelt', 'sklearn',
                        'pyclustering','matplotlib','scipy','wordcloud', 'fancyimpute', 'strconv'],
      classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
             'Topic :: Scientific/Engineering :: Information Analysis',
            'Programming Language :: Python :: 3'
      ],
      keywords="genomics big data pandas python",
      python_requires='>=3',
      include_package_data=True,
      zip_safe=False)
