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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681746356571 = glueContext.create_dynamic_frame.from_catalog(
    database="jai-test",
    table_name="jai_inputdata_bucket",
    transformation_ctx="AWSGlueDataCatalog_node1681746356571",
    additional_options = {"JobBookmarkKeys":["emp_id"],"JobBookmarkKeysSortOrder":"asc"}
)

# Script generated for node Amazon S3
AmazonS3_node1681746364329 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueDataCatalog_node1681746356571,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://jk-output", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1681746364329",
)

job.commit()
