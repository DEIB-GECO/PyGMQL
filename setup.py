from setuptools import setup

setup(name='gmql',
      version='0.1',
      description='Python library for GMQL computation',
      url='https://github.com/lucananni93/GMQL-Python',
      author='Luca Nanni',
      author_email='luca.nanni@mail.polimi.it',
      license='MIT',
      packages=['gmql'],
      requires=['findspark'],
      zip_safe=False)