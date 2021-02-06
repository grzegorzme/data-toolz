data-toolz
==========
This repository contains reusable python code for data projects


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
`datatoolz.io.DataIO` class gives you a versatile Reader/Writer interface for handling of typical data files (`jsonlines`, `tsv`, `parquet`)

```python
import pandas as pd
from datatoolz.io import DataIO

df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

dio = DataIO()  # defaults to "local" FileSystem

# write as parquet
dio.write(dataframe=df, path="my-file.parquet", filetype="parquet")
df_read = dio.read(path="my-file.parquet", filetype="parquet")

# write as gzip-compressed jsonlines
dio.write(dataframe=df, path="my-file.json.gz", filetype="jsonlines", gzip=True)
df_read = dio.read(path="my-file.json.gz", filetype="jsonlines", gzip=True)

# write as tab-separated-values in multiple partitions
dio.write(dataframe=df, path="my-file.tsv", filetype="tsv", partition_by=["col1"])
df_read = dio.read(path="my-file.json.gz", filetype="tsv")
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