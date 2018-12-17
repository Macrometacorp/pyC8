from setuptools import setup, find_packages

with open('./README.md') as fp:
    description = fp.read()

setup(
    name='pyC8',
    version='0.10.2',
    description='Python Driver for Macrometa Digital Edge Fabric',
    long_description=description,
    author='Macrometa',
    author_email='info@macrometa.co',
    url='https://www.macrometa.io',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=['requests', 'six', 'pulsar-client==2.1.0'],
    tests_require=['pytest', 'mock', 'flake8'],
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Documentation :: Sphinx'
    ]
)
