# Test data

`alert_sample.parquet` holds 12 alerts cut from Neven Caplar's parquet dump
of the alert archive (`/sdf/data/rubin/user/ncaplar/alert-parquet` on S3DF),
written with 6 rows per row group:

| Rows | Source file | Selection |
|---|---|---|
| 0–3 | `170055__00000000-00023434.parquet` | solar-system alerts (`diaSource.diaObjectId == 0`) |
| 4–7 | `314046__00000000-00004168.parquet` | `prvDiaSources` is null |
| 8–11 | `314046__00000000-00004168.parquet` | `prvDiaSources` has 1–3 entries |

To regenerate it:

```python
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

D = "/sdf/data/rubin/user/ncaplar/alert-parquet/"
a = pq.ParquetFile(D + "170055__00000000-00023434.parquet").read()
b = pq.ParquetFile(D + "314046__00000000-00004168.parquet").read()
ss = a.filter(pc.equal(pc.struct_field(a["diaSource"], "diaObjectId"), 0)).slice(0, 4)
lens = pc.list_value_length(b["prvDiaSources"])
null_history = b.filter(pc.is_null(lens)).slice(0, 4)
history = b.filter(pc.and_(pc.greater(lens, 0), pc.less_equal(lens, 3))).slice(0, 4)
table = pa.concat_tables([ss, null_history, history])
pq.write_table(table, "alert_sample.parquet", compression="zstd", row_group_size=6)
```

## Raw Avro alerts

`avro_alerts/` holds three real alerts, each in a different alert format,
copied unchanged from the raw archive on S3DF. Like the archive, it has
one gzipped Avro message per alert, at `<subdir>/<diaSourceId>.avro.gz`:

| Subdir | Filename | Format |
|---|---|---|
| `170019` | `170019695985952427.avro.gz` | 1000 |
| `170450` | `170450700567839112.avro.gz` | 1100 |
| `171167` | `171167627158226197.avro.gz` | 1101 |

The converter's tests read them in place, and copy the directory to use as a
small archive.
