# PyC8 SDK

![PyPI](https://img.shields.io/pypi/v/pyC8)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyc8)
![PyPI - Format](https://img.shields.io/pypi/format/pyc8)
![PyPI - Wheel](https://img.shields.io/pypi/wheel/pyc8)
![PyPI - Downloads](https://img.shields.io/pypi/dm/pyc8)

Python SDK for the Macrometa Global Data Network.

---

## 🐍 Supported Python Versions
This SDK supports the following Python implementations:

* Python 3.4 - 3.10

## ⚙️ Installation

Install from PyPi using [pip](https://pip.pypa.io/en/latest/), a
package manager for Python.

```commandline
pip install pyC8
```

After that you can check out our [getting started code examples](GETTING_STARTED.md).

## 🧶 Development environment
To enable development environment position ourselves to project's root and run:

```bash
pip install -r requirements/dev.txt
```

## 🧪 Testing

End-to-end tests can be found in tests/.
Before first run create .env file in tests/.
In `.env` file add variables:

* `FEDERATION_URL="<your federation url>"`
* `FABRIC="<selected fabric>"`
* `TENANT_EMAIL="<your tenant email>"`
* `TENANT_PASSWORD="<your tenant password>"`
* `API_KEY="<your api key>"`
* `TOKEN="<your token>"`
* `MM_TENANT_EMAIL="<Macrometa tenant email>"`
* `MM_TENANT_PASSWORD="<Macrometa tenant password>"`
* `MM_API_KEY="<Macrometa apy key>"`

**Note: MM_TENANT_EMAIL, MM_TENANT_PASSWORD, MM_API_KEY are super user credentials**

`.env` file is in `.gitignore`.

To run tests position yourself in the project's root while your virtual environment
is active and run:
```bash
python -m pytest
```

## 📐 Enable pre-commit hooks

You will need to install pre-commit hooks
Using homebrew:
```bash
brew install pre-commit
```
Using conda (via conda-forge):
```bash
conda install -c conda-forge pre-commit
```
To check installation run:
```bash
pre-commit --version
```
If installation was successful you will see version number.
You can find the Pre-commit configuration in `.pre-commit-config.yaml`.
Install the git hook scripts:
```bash
pre-commit install
```
Run against all files:
```bash
pre-commit run --all-files
```
If setup was successful pre-commit will run on every commit.
Every time you clone a project that uses pre-commit, running `pre-commit install`
should be the first thing you do.

## 👨‍💻 Build

To build package we need to position ourselves to project's root and run:

```bash
 $ python setup.py build
```

## 🪛 Upgrade
```bash
pip install --upgrade pyc8
```

## 📗 Examples

You can find code examples in our [getting started collection](GETTING_STARTED.md).

## 📜 Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report unacceptable behavior to [support@macrometa.com](mailto:support@macrometa.com).
