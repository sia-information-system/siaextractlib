API Reference
---

**Note:** Minimal documentation. One more detailed will be added later.

# Extractors

## Base

### BaseExtractor

Not intended to be instanced, but derived. This is an abstract class that
implements some common processes of an standard extractor.

``` python
class siaextractlib.extractors.base_extractor.BaseExtractor(
  log_stream = sys.stderr,
  verbose: bool = False
)
```

**Methods**

* `extract`

Asynchronous call of "sync_extract(...)" method.

``` python
def extract(self, filepath: Path | str, success_callback: Callable[[ExtractionDetails], None], failure_callback: Callable[[Exception], None]) -> None:
```

* `wait`

Waits until the `process_name` process ends. The `process_name`
is the name of an async method.

``` python
def wait(self, process_name: str, seconds: float = None) -> None:
```

* `still_working`

Returns True if the `process_name` process is still running.
The `process_name` is the name of an async method.

``` python
def still_working(self, process_name: str) -> bool:
```

* `log`

Prints logs to the self.log_stream. This stream does not need to be
a real stream. Just need to implements the .write(...) method.
The arguments are the same than in print(...) standard function.

``` python
def log(self, *args, **kwargs):
```

## OPeNDAP

### OpendapExtractor

A concrete class to extract data from OPeNDAP servers.
Derived from `siaextractlib.extractors.base_extractor.BaseExtractor`.

``` python
class siaextractlib.extractors.OpendapExtractor(
  opendap_url: str,
  auth: SimpleAuth = None,
  dim_constraints: dict[str, slice | list] = None,
  requested_vars: list[str] = None,
  log_stream = sys.stderr,
  max_attempts: int = 5, # per block
  req_max_size: int = 64, # MB, per block
  verbose: bool = False
)
```

**Methods**

* `connect`

Asynchronous call of "sync_connect(...)" method.

``` python
def connect(success_callback: Callable[..., None], failure_callback: Callable[[BaseException], None], **kwargs):
```

* `sync_connect`

Generates a Pydap connection and open the dataset with it.
**kwargs are forwarded to `xr.open_dataset` method.

``` python
def sync_connect(**kwargs)
```

* `close`

Closes the connection with the remote dataset.

``` python
def close():
```

* `get_size`

Returns the size of the dataset based on the current constraints.

``` python
def get_size(self, unit: SizeUnit = SizeUnit.BYTE) -> RequestSize:
```

* `sync_extract`

Executes the extraction by splitting the request size in blocks of self.req_max_size size.
Once all blocks has been downloaded, it merges them all in a new single file.

``` python
def sync_extract(self, filepath: Path | str) -> ExtractionDetails:
```

* `forget_tmp_files`

Clean the in-between file list without unlink them.

``` python
def forget_tmp_files(self):
```

* `unlink_tmp_files`

Remove the in-between files generated.

``` python
def unlink_tmp_files(self):
```
