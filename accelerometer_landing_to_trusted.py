import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1769968503150 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1769968503150")

# Script generated for node Customer Trusted
CustomerTrusted_node1769968574421 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1769968574421")

# Script generated for node Filter opt-in customers
SqlQuery2086 = '''
select accelerometer_landing.*
from accelerometer_landing
  join customer_trusted on customer_trusted.email = accelerometer_landing.user;
'''
Filteroptincustomers_node1769968786061 = sparkSqlQuery(glueContext, query = SqlQuery2086, mapping = {"accelerometer_landing":Accelerometerlanding_node1769968503150, "customer_trusted":CustomerTrusted_node1769968574421}, transformation_ctx = "Filteroptincustomers_node1769968786061")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1769968887077 = glueContext.getSink(path="s3://dredm11-stedi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1769968887077")
Accelerometertrusted_node1769968887077.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1769968887077.setFormat("json")
Accelerometertrusted_node1769968887077.writeFrame(Filteroptincustomers_node1769968786061)
job.commit()