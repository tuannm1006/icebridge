from icebridge.client import IceBridgeClient

client = IceBridgeClient()
gateway = client.gateway
jvm = client.jvm()

hadoop_conf = jvm.org.apache.hadoop.conf.Configuration

hadoop_catalog = jvm.org.apache.iceberg.hadoop.HadoopCatalog

catalog = hadoop_catalog(hadoop_conf(), "demo")  

namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)

namespace_vargs[0] = "bsc"

namespace = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace, "logs")

table = catalog.loadTable(identifier)

result = jvm.org.apache.iceberg.data.IcebergGenerics.read(table).build()