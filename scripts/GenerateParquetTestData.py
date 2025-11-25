import pyarrow as pa
import pyarrow.parquet as pq
import random

TableSize = 10000000

DataList = []
for i in range(TableSize):
    if i % 10 == 0:
        DataList.append(random.randint(150, 200))
    else:
        DataList.append(random.randint(0, 90))

IntColumnData = pa.array(DataList, type=pa.int32())

Schema = pa.schema([
    pa.field('IntColumn', pa.int32())
])

Table = pa.Table.from_arrays([IntColumnData], schema=Schema)

pq.write_table(Table, 'TEST_DATA.parquet')

print(f"Successfully created 'TEST_DATA.parquet' with {TableSize} rows.")