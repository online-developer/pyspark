from string import punctuation
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, udf, regexp_replace, trim, concat, lit, when, min, lag
from pyspark.sql.window import Window
from chain import chain
from hashlib import md5


# Patch DataFrame.transform()
def transform(self, f):
    return f(self)
DataFrame.transform = transform


def _lrtrim(c):
    return str(c).strip()
u_lrtrim = udf(_lrtrim)


def _remove_punctuations(c):
    return c.encode('ascii', 'ignore').translate(None, punctuation)
u_remove_punctuations = udf(_remove_punctuations)


def _single_space(c):
    return trim(regexp_replace(c,' +', ' '))
u_single_space = udf(_single_space)


def _hash_md5(c):
    return md5(str(c).encode('utf-8')).hexdigest().upper()
u_hash_md5 = udf(_hash_md5)



@chain
def remove_punctuations(col_name,df):
    return df.withColumn(col_name, u_remove_punctuations(col(col_name)))


@chain
def hash_md5(col_name, cols, df):
    key = col(cols[0])
    for i in range(1, len(cols)):
        key = concat(key, col(cols[i]))
    return df.withColumn(col_name, u_hash_md5(key))


@chain
def lrtrim(cols, df):
    if cols == []:
        cols = df.columns
    for c in cols:
        df = df.withColumn(c, u_lrtrim(c))
    return df


@chain
def add_default(col_name, val, df):
    return df.withColumn(col_name, lit(val))


@chain
def rename_cols(prefix, df):
    for c in df.columns:
        if c == 'hash_key':
            col_name = c
        else:
            col_name = prefix + c
        df = df.withColumnRenamed(c, col_name)

    return df


@chain
def find_new_recs(col_start_dt,col_end_dt,cols,df_new,df_old):
    w = Window.partitionBy(col('new.hash_key')).orderBy((col('new.'+col_start_dt)).desc())
    return df_new.alias('new').join(df_old.alias('old'), 'hash_key', how='left') \
                             .filter(col('old.hash_key').isNull()) \
                             .select("*", lag(col('new.'+col_start_dt)).over(w).alias('_new_end_dt')) \
                             .select([col('new.'+c) for c in cols] + [col('_new_end_dt')]) \
                             .withColumn(col_end_dt, when(col('_new_end_dt').isNull(), col(col_end_dt)) \
                                                  .otherwise(col('_new_end_dt'))) \
                             .drop(col('_new_end_dt')) \
                             .withColumn('rec_typ', lit('N'))


@chain
def find_unchanged_recs(cols,df_new,df_old):
    return df_new.alias('new').join(df_old.alias('old'), 'hash_key', how='right') \
                             .filter(col('new.hash_key').isNull()) \
                             .select([col('old.'+c) for c in cols]) \
                             .withColumn('rec_typ', lit('O'))



@chain
def find_changed_recs(col_start_dt,col_end_dt,cols,rec_typ,df_new,df_old):
    if rec_typ == 'new':
        w = Window.partitionBy(col('new.hash_key')).orderBy((col('new.'+col_start_dt)).desc())
        return df_new.alias('new').join(df_old.alias('old'), 'hash_key', how='inner') \
                                .filter(col('old.'+col_end_dt) == '9999-12-31') \
                                .select("*", lag(col('new.'+col_start_dt)).over(w).alias('_new_end_dt')) \
                                .select([col('new.'+c) for c in cols] + [col('_new_end_dt')]) \
                                .withColumn(col_end_dt, when(col('_new_end_dt').isNull(), col(col_end_dt)) \
                                                     .otherwise(col('_new_end_dt'))) \
                                .drop(col('_new_end_dt')) \
                                .withColumn('rec_typ', lit('F'))
    else:
        return df_new.groupBy('hash_key').agg(min(col(col_start_dt)).alias(col_start_dt)).alias('new') \
                                .join(df_old.alias('old'), 'hash_key', how='inner') \
                                .withColumn('_new_end_dt', when(col('old.' + col_end_dt) == '9999-12-31', (col('new.' + col_start_dt))) \
                                                         .otherwise(col('old.' + col_end_dt))) \
                                .select([col('old.'+c) for c in cols if c != col_end_dt] + [col('_new_end_dt')]) \
                                .withColumnRenamed('_new_end_dt', col_end_dt) \
                                .withColumn('rec_typ', lit('L'))


@chain
def merge_recs(sort_key, sort_order, df_new, df_upd_new, df_upd_old, df_unchanged):
    return df_new.unionAll(df_upd_new) \
                .unionAll(df_upd_old) \
                .unionAll(df_unchanged) \
                .orderBy(sort_key, ascending=sort_order)




