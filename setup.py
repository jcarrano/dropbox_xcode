from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='dropbox-xcode',
      version='0.1.4',
      description='Download and transcode audio files from Dropbox',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Intended Audience :: End Users/Desktop',
        'Topic :: Multimedia :: Sound/Audio :: Conversion',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: System :: Archiving :: Compression'
      ],
      keywords='dropbox flac opus sync',
      url='https://github.com/jcarrano/dropbox_xcode',
      author='Juan I Carrano <juan@carrano.com.ar>',
      author_email='juan@carrano.com.ar',
      license='MIT',
      install_requires=[
          'dropbox', 'tqdm', 'mutagen'
      ],
      py_modules=['dropbox_xcode'],
      entry_points = {
        'console_scripts': ['dropbox-xcode=dropbox_xcode:main'],
      },
      include_package_data=True,
      zip_safe=True
)
