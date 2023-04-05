# Tests

This test package uses the framework `unittest` from the python standard library.

You can execute a whole test module as a normal python script with:

```sh
python [module]
```

For example:

```sh
python test_lib_opendap.py
```

Or execute specific classes or even specific methods of a class derived from unittest.TestCase class with:

```sh
python -m unittest [test_module].[class].[method]
```

For example:

```sh
python -m unittest test_lib_opendap.TestCopernicusOpendap.test_4mb_donwload_sync
```

See the unittest docs for more information.

# References

* unittest module: https://docs.python.org/3/library/unittest.html

