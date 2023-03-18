import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step trainer landing
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance-analytics",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Customer curated
Customercurated_node1679062465719 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance-analytics",
    table_name="customer_curated",
    transformation_ctx="Customercurated_node1679062465719",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1679089328167 = DynamicFrame.fromDF(
    Customercurated_node1679062465719.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1679089328167",
)

# Script generated for node Join
Join_node1679063871304 = Join.apply(
    frame1=Steptrainerlanding_node1,
    frame2=DropDuplicates_node1679089328167,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1679063871304",
)

# Script generated for node Amazon S3
AmazonS3_node1679063895695 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1679063871304,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-mw/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1679063895695",
)

job.commit()