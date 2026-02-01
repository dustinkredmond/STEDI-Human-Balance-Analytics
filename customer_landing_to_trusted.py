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

# Script generated for node Customer Landing Zone S3
CustomerLandingZoneS3_node1769967607919 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingZoneS3_node1769967607919")

# Script generated for node Filter opt-in research
SqlQuery2160 = '''
select * from myDataSource
where sharewithresearchasofdate <> 0;

'''
Filteroptinresearch_node1769967753718 = sparkSqlQuery(glueContext, query = SqlQuery2160, mapping = {"myDataSource":CustomerLandingZoneS3_node1769967607919}, transformation_ctx = "Filteroptinresearch_node1769967753718")

# Script generated for node Customer Trusted Zone S3
CustomerTrustedZoneS3_node1769967793301 = glueContext.getSink(path="s3://dredm11-stedi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedZoneS3_node1769967793301")
CustomerTrustedZoneS3_node1769967793301.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrustedZoneS3_node1769967793301.setFormat("json")
CustomerTrustedZoneS3_node1769967793301.writeFrame(Filteroptinresearch_node1769967753718)
job.commit()