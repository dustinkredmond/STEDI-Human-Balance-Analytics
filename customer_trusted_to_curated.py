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

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1769970543349 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLandingZone_node1769970543349")

# Script generated for node Customers Trusted Zone
CustomersTrustedZone_node1769970485437 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/customer/trusted/"], "recurse": True}, transformation_ctx="CustomersTrustedZone_node1769970485437")

# Script generated for node SQL Query
SqlQuery2503 = '''
select distinct customer_trusted.* 
from customer_trusted
  join accelerometer_landing on accelerometer_landing.user = customer_trusted.email;
'''
SQLQuery_node1769970601246 = sparkSqlQuery(glueContext, query = SqlQuery2503, mapping = {"accelerometer_landing":AccelerometerLandingZone_node1769970543349, "customer_trusted":CustomersTrustedZone_node1769970485437}, transformation_ctx = "SQLQuery_node1769970601246")

# Script generated for node Customer Curated
CustomerCurated_node1769971244606 = glueContext.getSink(path="s3://dredm11-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1769971244606")
CustomerCurated_node1769971244606.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1769971244606.setFormat("json")
CustomerCurated_node1769971244606.writeFrame(SQLQuery_node1769970601246)
job.commit()