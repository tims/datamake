from setuptools import setup
setup(name='datamake',
  version='0.2.4',
  author='Tim Sell',
  author_email='trsell@gmail.com',
  url='https://github.com/tims/datamake',
  packages=['datamake'],
  scripts=['scripts/datamake','scripts/datamakeold'],
  license='LICENSE.txt',
  install_requires=['requests','boto','pyparseuri','oursql','networkx','webhdfs-py'],
  test_suite='test'
)
