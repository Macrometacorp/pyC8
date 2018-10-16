## Build & Install

To build,

```bash
 $ python setup.py build
```
To install locally,

```bash
 $ python setup.py build
```

## Package and Make available through pip

Requirements,

```bash
 $ python3 -m pip install --user --upgrade setuptools wheel
 $ python3 -m pip install --user --upgrade twine
```

Run following command from directory where setup.py is present.

```base
 $ python3 setup.py sdist bdist_wheel
```

### To upload to test.pypi.org

Initally Upload the Distribution to Test Archives test.pypi.org for testing purposes. This step will prompt you for username and password.

* username: `macrometaco`
* password: `poweruser!@#`

```bash
 $ twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

The above test distribution you uploaded can be installed using:

```bash
 $ pip3 install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple pyc8
```

### To upload to pip.org

Upload the Distribution Archives to pip.org. This step will prompt you for username and password.

* username: `macrometaco`
* password: `poweruser!@#`

```bash
 $ twine upload dist/*
```

You may need to use `sudo` depending on your environment.
