data-toolz
==========
This repository contains reusable python code for data projects.

The motivation for this project was to create a package which allows to abstract dataset read/write operations from 
* destination type (`local`, `s3`, `<tbd...>`) and 
* target file type (`delimiter-separated values`, `jsonlines`, `parquet`)

This would allow to write code easily transferable between local and cloud applications.


installation
============
```shell script
pip install data-toolz
```

usage
=====

`datatoolz.filesystem.FileSystem` class gives you an abstraction for accesing both local and remote object using the well know pythonic `open()` interface.

```python
from datatoolz.filesystem import FileSystem

for fs_type in ("local", "s3"):
    fs = FileSystem(name=fs_type)

    # common pythonic interface for both local and remote file systems
    with fs.open("my-folder-or-bucket/my-file", mode="wt") as fo:
        fo.write("Hello World!")
```
---
`datatoolz.io.DataIO` class gives you a versatile Reader/Writer interface for handling of typical data files (`jsonlines`, `dsv`, `parquet`)

```python
import pandas as pd
from datatoolz.io import DataIO

df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

dio = DataIO()  # defaults to "local" FileSystem

# write as parquet
dio.write(dataframe=df, path="my-file.parquet", filetype="parquet")
dio.read(path="my-file.parquet", filetype="parquet")

# write as gzip-compressed jsonlines
dio.write(dataframe=df, path="my-file.json.gz", filetype="jsonlines", gzip=True)
dio.read(path="my-file.json.gz", filetype="jsonlines", gzip=True)

# write as delimiter-separated-values in multiple partitions
dio.write(dataframe=df, path="my-file.tsv", filetype="dsv", sep="\t", partition_by=["col1"])
dio.read(path="my-file.tsv", filetype="dsv", sep="\t")

# write output in multiple chunks per partition
dio.write(dataframe=df, path="my-prefix", filetype="dsv", sep="\t", partition_by=["col1"], suffix=["chunk01.tsv", "chunk02.tsv"])
dio.read(path="my-prefix", filetype="dsv", sep="\t")
```
---
`datatoolz.logging.JsonLogger` is a wrapper logger for outputting JSON-structured logs
```python
from datatoolz.logging import JsonLogger

logger = JsonLogger(name="my-custom-logger", env="dev")
logger.info(msg="what is my purpose?", meaning_of_life=42)
```
```
{"logger": {"application": "my-custom-logger", "environment": "dev"}, "level": "info", "timestamp": "2020-11-03 18:31:07.757534", "message": "what is my purpose?", "extra": {"meaning_of_life": 42}}
```
It can also be used to decorate functions and log their execution details
```python
from datatoolz.logging import JsonLogger

logger = JsonLogger(name="my-custom-logger", env="dev")

@logger.decorate(msg="my-custom-log", duration=True, memory=True, my_value="my-value", output_length=lambda x: len(x))
def my_func(x, y):
    return x + y, x * y

print(my_func(42, 2))
```
```
{"logger": {"application": "my-custom-logger", "environment": "dev"}, "level": "info", "timestamp": "2021-03-24 18:10:47.054703", "message": "my-custom-log", "extra": {"function": "my_func", "memory": {"current": 432, "peak": 432}, "duration": 2.5980000000203063e-06, "my_value": "my-value", "output_length": 2}}
(44, 84)
```
