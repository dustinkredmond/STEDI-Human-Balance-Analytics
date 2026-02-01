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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1769971657629 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/step_trainer/landing"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1769971657629")

# Script generated for node Customer Curated
CustomerCurated_node1769971706045 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1769971706045")

# Script generated for node SQL Query
SqlQuery2005 = '''
select step_trainer_landing.*
from step_trainer_landing
  join customer_curated on customer_curated.serialnumber = step_trainer_landing.serialnumber;
'''
SQLQuery_node1769971724588 = sparkSqlQuery(glueContext, query = SqlQuery2005, mapping = {"customer_curated":CustomerCurated_node1769971706045, "step_trainer_landing":StepTrainerLanding_node1769971657629}, transformation_ctx = "SQLQuery_node1769971724588")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1769971782981 = glueContext.getSink(path="s3://dredm11-stedi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1769971782981")
StepTrainerTrusted_node1769971782981.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1769971782981.setFormat("json")
StepTrainerTrusted_node1769971782981.writeFrame(SQLQuery_node1769971724588)
job.commit()