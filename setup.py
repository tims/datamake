from distutils.core import setup
setup(name='datamake',
  version='0.9.0',
  author='Tim Sell',
  author_email='trsell@gmail.com',
  url='https://github.com/tims/datamake',
  py_modules=['datamake'],
  scripts=['scripts/datamake','scripts/datamakenew'],
  license='LICENSE.txt',
  install_requires=['urllib3','requests','boto','pyparseuri','oursql']
)