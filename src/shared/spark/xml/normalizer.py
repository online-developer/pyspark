from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType
import xml.etree.ElementTree as ET


def get_xpath_element(xpath, namespace=None, ignore_root=False):
    if namespace:
        root = './' + namespace
        seperator = '/' + namespace
    else:
        root = './'
        seperator = '/'

    num_of_elements = xpath.count('/') if xpath.count('/') > 0 else 0

    if num_of_elements > 1:
        if ignore_root:
            return (root + seperator.join(xpath.split('/')[1:]))
        else:
            return (root + seperator.join(xpath.split('/')[2:]))
    elif num_of_elements == 1:
        if ignore_root:
            return (root + xpath.split('/')[1])

    return ('.')


def get_xpath_element_name(xpath):
    return (xpath.split('/')[-1])


def get_xpath_value(xml_data, xpath, namespace):
    child_name = get_xpath_element_name(xpath)
    is_attr = False if not child_name.startswith('@') else True
    if is_attr:
        xpath = '/'.join(xpath.split('/')[:-1])

    xpath = get_xpath_element(xpath, namespace, ignore_root=True)
    xml_value = xml_data.find(xpath)
    if is_attr:
        attr = child_name.replace('@', '')
        try:
            return xml_value.get(attr)
        except:
            return None
    else:
        try:
            # print ('   {}={}'.format(xpath, xml_value.text))
            return xml_value.text
        except:
            return None


def get_relative_xpath(parent, child):
    len_parent = len(parent.split('/'))
    return '/' + '/'.join(child.split('/')[len_parent:])


def get_parent(relationships, table):
    for parent in relationships:
        if table in relationships[parent]['childrens']:
            return parent


def infer_schema(cols):
    return StructType([StructField(str(col), StringType(), True) for col in cols])


def create_records_from_xml(data, tables, table, table_xpath, cols, keys, namespace, generate_pk):
    row = dict()
    records = list()
    row_num = 0

    for table_data in data.findall(table_xpath):
        fields = dict()
        if keys:
            fields.update(keys)
        if generate_pk:
            row_num += 1
            fields['PK_' + get_xpath_element_name(table)] = row_num
        for field in tables[table]:
            key = tables[table][field]
            field_relative_xpath = get_relative_xpath(table, field)
            # print('{}={}'.format(parent_xpath, child_relative_xpath))
            field_value = get_xpath_value(table_data, field_relative_xpath, namespace)
            fields[key] = field_value
        row[cols[0]] = get_xpath_element_name(table)
        row[cols[1]] = fields
        records.append(Row(**row))
    return records


def rowify(xml, cols, tables, namespace, relationships):
    """creates a Row object conforming to a schema as specified by a dict"""
    data = ET.fromstring(xml.encode('utf-8'))
    records = list()
    parent = None
    for table in tables:
        fk_row_num = 0
        keys = dict()
        generate_pk = True if table in relationships else False
        parent = get_parent(relationships, table)
        generate_fk = True if parent else False
        if parent:
            parent_xpath = get_xpath_element(parent, namespace)
            for parent_data in data.findall(parent_xpath):
                if generate_fk:
                    fk_row_num += 1
                keys['FK_' + get_xpath_element_name(parent)] = fk_row_num
                table_relative_path = get_relative_xpath(parent, table)
                table_relative_xpath = get_xpath_element(table_relative_path, namespace, True)
                records.extend(create_records_from_xml(
                  parent_data, tables, table, table_relative_xpath, cols, keys, namespace, generate_pk)
                )
        else:
            table_xpath = get_xpath_element(table, namespace)
            records.extend(create_records_from_xml(
              data, tables, table, table_xpath, cols, keys, namespace, generate_pk)
            )

    return (records)


def normalize_rdd(rdd, cols, app_schema, namespace, relationships):
    xpaths = app_schema
    row_rdd = rdd.flatMap(
        lambda x: rowify(x[1], cols, xpaths, namespace, relationships)
    )
    return row_rdd


def df_from_rdd(rdd, cols, sql):
    """creates a dataframe out of an rdd """
    schema = infer_schema(cols)
    return sql.createDataFrame(rdd, schema)
