from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType
import json
import xml.etree.ElementTree as ET


def get_xml_element(xpath):
    return ('./' + '/'.join(xpath.split('/')[2:]))


def get_xml_element_name(xpath):
    return (xpath.split('/')[-1])


def infer_schema(cols):
    return StructType([StructField(str(col), StringType(), True) for col in cols])


def rowify(xml, cols, xpaths, row_num):
    """creates a Row object conforming to a schema as specified by a dict"""
    data = ET.fromstring(xml.encode('utf-8'))
    records = list()
    for parent in xpaths:
        parent_key = get_xml_element(parent)
        num_of_childrens = (len(data.findall(parent_key)))
        row_num = 0
        row = dict()
        while row_num < num_of_childrens:
            fields = dict()
            for child in xpaths[parent]:
                key = xpaths[parent][child]
                child_key = get_xml_element(child)
                value = (data.findall(child_key)[row_num].text)
                fields[key] = value
            row_num += 1
            row[cols[0]] = get_xml_element_name(parent)
            row[cols[1]] = fields
            records.append(Row(**row))
    return (records)


def df_from_rdd(rdd, cols, row_num, sql, schema_file):

    """creates a dataframe out of an rdd """
    schema = infer_schema(cols)
    xpaths = json.load(open(schema_file, 'r'))
    row_rdd = rdd.flatMap(lambda x: rowify(x[1], cols, xpaths, row_num))
    return sql.createDataFrame(row_rdd, schema)


