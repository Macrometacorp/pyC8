from setuptools import find_packages, setup

with open("./README.md") as fp:
    description = fp.read()

setup(
    name="pyC8",
    version='0.18.0',
    description="Python SDK for Macrometa Global Data Mesh",
    long_description=description,
    long_description_content_type="text/markdown",
    author="Macrometa",
    author_email="product@macrometa.com",
    url="https://www.macrometa.com",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=["requests==2.25.1", "six", "websocket-client==0.57.0"],
    tests_require=["pytest", "mock", "flake8"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Information Technology",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Documentation :: Sphinx",
    ],
)
