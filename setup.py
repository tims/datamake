from distutils.core import setup
setup(name='datamake',
  version='0.1.0',
  author='Tim Sell',
  author_email='trsell@gmail.com',
  url='https://github.com/tims/datamake',
  packages=['datamake'],
  scripts=['scripts/datamake','scripts/datamakenew'],
  license='LICENSE.txt',
  install_requires=['requests','boto','pyparseuri','oursql','networkx']
)