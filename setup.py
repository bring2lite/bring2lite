from setuptools import setup

setup(name='bring2lite',
      package_dir={
            'bring2lite': 'bring2lite',
            'bring2lite.classes': 'bring2lite/classes',
      },
      packages=['bring2lite', 'bring2lite.classes'],
      entry_points={
          'console_scripts': ['bring2lite=bring2lite.main:main']
      },
      version='0.1',
      description='extract deleted content out of sqlite databases',
      url='',
      author='Christian Meng,'
             'Harald Baier',
      author_email='bring2lite@gmail.com',
      license='MIT',
      install_requires=[
          'tqdm',
          'sqlparse',
          'colorama',
          'pyqt5',
      ],
      hiddenimports=[
          'PyQt5.sip'
      ],
      zip_safe=True)
