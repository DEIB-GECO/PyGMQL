from setuptools import setup, find_packages
import os

with open(os.path.join(".", "gmql", "resources", "version"), "r") as f_ver:
    version = f_ver.read().strip()

with open(os.path.join(".", "gmql", "resources", "github_url"), "r") as f_ver:
    github_url = f_ver.read().strip()

setup(name='gmql',
      version=version,
      description='Python library for GMQL',
      long_description="PyGMQL is a python module that enables the user "
                       "to perform operation on genomic data in a scalable way.",
      url=github_url,
      author='Luca Nanni',
      author_email='luca.nanni@mail.polimi.it',
      license='MIT',
      download_url='{}/tarball/{}'.format(github_url, version),
      packages=find_packages(),
      install_requires=['pandas', 'tqdm', 'numpy', 'py4j',
                        'psutil', 'requests', 'requests-toolbelt'],
      classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
            
            'Programming Language :: Python :: 3'
      ],
      keywords="genomics big data pandas python",
      python_requires='>=3',
      include_package_data=True,
      zip_safe=False)
