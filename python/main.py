from icebridge.client import IceBridgeClient
from py4j.java_collections import ListConverter

client = IceBridgeClient()
gateway = client.gateway
jvm = client.jvm()

# # # create table # # #

fields = [
    jvm.org.apache.iceberg.types.Types.NestedField.optional(1, "username", jvm.org.apache.iceberg.types.Types.StringType.get()),
    jvm.org.apache.iceberg.types.Types.NestedField.optional(2, "age", jvm.org.apache.iceberg.types.Types.IntegerType.get())
]

java_fields = ListConverter().convert(fields, gateway._gateway_client)

java_schema = jvm.org.apache.iceberg.Schema(java_fields)

hadoop_conf = jvm.org.apache.hadoop.conf.Configuration

hadoop_catalog = jvm.org.apache.iceberg.hadoop.HadoopCatalog

catalog = hadoop_catalog(hadoop_conf(), "demo")  

namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)

namespace_vargs[0] = "bsc"

namespace = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace, "logs")

# catalog.createTable(identifier, java_schema)

# # # create table # # #

table = catalog.loadTable(identifier)

# # # insert data # # #

record = jvm.org.apache.iceberg.data.GenericRecord.create(java_schema)

builder = jvm.com.google.common.collect.ImmutableList.builder()

builder.add(record.copy(jvm.com.google.common.collect.ImmutableMap.of("username", "Bruce", "age", 19)))
builder.add(record.copy(jvm.com.google.common.collect.ImmutableMap.of("username", "Wayne", "age", 18)))
builder.add(record.copy(jvm.com.google.common.collect.ImmutableMap.of("username", "Clark", "age", 20)))

records = builder.build()

filepath = table.location() + "/" + jvm.java.util.UUID.randomUUID().toString()
file = table.io().newOutputFile(filepath)

# createWriterFunc = gateway.new_array(jvm.java.util.function.Function, 1)
# createWriterFunc[0] = jvm.org.apache.iceberg.data.parquet.GenericParquetWriter.buildWriter

dataWriter = jvm.org.apache.iceberg.parquet.Parquet.writeData(file) \
    .schema(java_schema) \
    .createWriterFunc(lambda: jvm.org.apache.iceberg.data.parquet.GenericParquetWriter.buildWriter()) \
    .overwrite() \
    .withSpec(jvm.org.apache.iceberg.PartitionSpec.unpartitioned()) \
    .build()

try:
    for record in records:
        dataWriter.write(record)
finally:
    dataWriter.close()

# # # insert data # # #