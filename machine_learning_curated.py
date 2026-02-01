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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1769972190924 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1769972190924")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1769972157999 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dredm11-stedi/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1769972157999")

# Script generated for node Join Trusted
SqlQuery2278 = '''
select step_trainer_trusted.*, accelerometer_trusted.*
from step_trainer_trusted
  join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;
'''
JoinTrusted_node1769972222094 = sparkSqlQuery(glueContext, query = SqlQuery2278, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1769972157999, "accelerometer_trusted":AccelerometerTrusted_node1769972190924}, transformation_ctx = "JoinTrusted_node1769972222094")

# Script generated for node Amazon S3
AmazonS3_node1769972270045 = glueContext.getSink(path="s3://dredm11-stedi/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769972270045")
AmazonS3_node1769972270045.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1769972270045.setFormat("json")
AmazonS3_node1769972270045.writeFrame(JoinTrusted_node1769972222094)
job.commit()