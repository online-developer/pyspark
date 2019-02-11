# String Functions

from string import punctuation
from pyspark.sql.functions import udf,col, regexp_replace
from pyspark.sql.dataframe import DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform


def _remove_punctuation(colName):
    return colName.encode('ascii','ignore').translate(None, punctuation)

udf_remove_punctuation = udf(_remove_punctuation)


def remove_punctuation(colName):
    def inner(df):
        return df.withColumn(colName,udf_remove_punctuation(col(colName)))
    return inner


