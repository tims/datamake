from distutils.core import setup
setup(name='datamake',
  version='0.0.1',
  author='Tim Sell',
  author_email='trsell@gmail.com',
  url='https://github.com/tims/datamake',
  py_modules=['datamake'],
  scripts=['scripts/datamake'],
  requires=['requests','boto','pyparseuri']
)