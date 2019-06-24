import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "catco", table_name = "glue_datapj", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "catco", table_name = "glue_datapj", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("policyid", "long", "policyid", "long"), ("statecode", "string", "statecode", "string"), ("county", "string", "county", "string"), ("eq_site_limit", "double", "eq_site_limit", "double"), ("hu_site_limit", "double", "hu_site_limit", "double"), ("fl_site_limit", "double", "fl_site_limit", "double"), ("fr_site_limit", "double", "fr_site_limit", "double"), ("tiv_2011", "double", "tiv_2011", "double"), ("tiv_2012", "double", "tiv_2012", "double"), ("eq_site_deductible", "double", "eq_site_deductible", "double"), ("hu_site_deductible", "double", "hu_site_deductible", "double"), ("fl_site_deductible", "long", "fl_site_deductible", "long"), ("fr_site_deductible", "long", "fr_site_deductible", "long"), ("point_latitude", "double", "point_latitude", "double"), ("point_longitude", "double", "point_longitude", "double"), ("line", "string", "line", "string"), ("construction", "string", "construction", "string"), ("point_granularity", "long", "point_granularity", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("policyid", "long", "policyid", "string"), ("statecode", "string", "statecode", "string"), ("county", "string", "county", "string"), ("eq_site_limit", "double", "eq_site_limit", "double"), ("hu_site_limit", "double", "hu_site_limit", "double"), ("fl_site_limit", "double", "fl_site_limit", "double"), ("fr_site_limit", "double", "fr_site_limit", "double"), ("tiv_2011", "double", "tiv_2011", "double"), ("tiv_2012", "double", "tiv_2012", "double"), ("eq_site_deductible", "double", "eq_site_deductible", "double"), ("hu_site_deductible", "double", "hu_site_deductible", "double"), ("fl_site_deductible", "long", "fl_site_deductible", "long"), ("fr_site_deductible", "long", "fr_site_deductible", "long"), ("point_latitude", "double", "point_latitude", "double"), ("point_longitude", "double", "point_longitude", "double"), ("line", "string", "line", "string"), ("construction", "string", "construction", "string"), ("point_granularity", "long", "point_granularity", "long")], transformation_ctx = "applymapping1")


## @type: ResolveChoice
## @return: applymapping1
## @inputs: [frame = datasource0]

applymapping1 = ResolveChoice.apply(datasource0, specs = [("policyid", "cast:long"), ("eq_site_limit", "cast:int")])


## @type: DataTransform
## @args: [transformation_ctx="transform_ctx"]
## @return: dataset
## @inputs: applymapping1
df=applymapping1.toDF()
data=df.groupby("county","statecode","construction").agg({'policyid':'sum','eq_site_limit':'max'}).select(col('county'),col('statecode'),col('construction'),col('sum(policyid)'),col('max(eq_site_limit)'))
dataset=DynamicFrame.fromDF(data,glueContext,"dataset")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://glue-datapj/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = dataset]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = dataset, connection_type = "s3", connection_options = {"path": "s3://glue-datapj/"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
