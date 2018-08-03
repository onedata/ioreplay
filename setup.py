from setuptools import setup


setup(name='ioreplay',
      version='0.2',
      author='Onedata Package Maintainer',
      author_email='info@onedata.org',
      description='application used to replay recorder Oneclient io activity',
      license='Apache License',
      url='https://onedata.org',
      packages=['ioreplay'],
      entry_points={
          'console_scripts': [
              'ioreplay = ioreplay.ioreplay:main'
          ]
      },
      python_requires='>=3.5.0',
      keywords='Onedata Oneclient io qa testing',
      classifiers=[
          'License :: OSI Approved :: Apache License',
          'Operating System :: POSIX',
          'Topic :: Software Development :: Quality Assurance',
          'Topic :: Software Development :: Testing',
          'Topic :: Utilities',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7'
      ])
