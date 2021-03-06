from pyspark.sql import SQLContext
from shared.spark.xml.normalizer import df_from_rdd
from shared.spark.xml.normalizer import normalize_rdd
from shared.context import JobContext
from .schema import app_schema
from .schema import namespace
from .schema import relationships
import os


class Xml2erJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'row_num')


def analyze(sc, **kwargs):
    print ("Running xml2df")
    # Create the singleton instance
    sql = SQLContext(sparkContext=sc)
    context = Xml2erJobContext(sc)

    # Default row number
    row_num = context.get_counter('row_num')
    cols = ['table', 'columns']
    cwd = os.environ.get('PYSPARK_JOB_DIR')
    rdd = sc.wholeTextFiles(os.path.join(cwd, 'agent.xml'))
    # schema_file = os.path.join(cwd, 'agent.json')
    df = df_from_rdd(
           normalize_rdd(
             rdd, cols, app_schema, namespace, relationships
           ),
           cols,
           sql
    )

    df.show(20, False)
