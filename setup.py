from distutils.core import setup
setup(name='datamake',
  version='0.0.3',
  author='Tim Sell',
  author_email='trsell@gmail.com',
  url='https://github.com/tims/datamake',
  py_modules=['datamake'],
  scripts=['scripts/datamake'],
  install_requires=['requests','boto','pyparseuri','simplejson']
)