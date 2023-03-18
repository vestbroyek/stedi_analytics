import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance-analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1678532687033 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance-analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1678532687033",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=CustomerTrusted_node1678532687033,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1678532948758 = DropFields.apply(
    frame=Join_node2,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1678532948758",
)

# Script generated for node Customer curated
Customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1678532948758,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-mw/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customercurated_node3",
)

job.commit()
