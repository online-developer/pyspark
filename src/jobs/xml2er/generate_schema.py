from shared.common.xml.parser import write_xml2json
from shared.common.xml.parser import get_xml_namespace
import json


def get_parent(parents, child, filters=None):
    for parent in parents:
        if child in parents[parent]['childrens']:
            if not filters or parent not in filters:
                return parent
    return None


def is_child(parent, child):
    loop = True
    end = -1
    while loop:
        element = '/'.join(child.split('/')[0:end])
        if parent == element:
            return True
        end -= 1
        if element.count('/') < 1:
            loop = False
    return False


def generate_relationships(sorted_app_schema):

    relationships = dict()
    for parent in sorted_app_schema:
        childrens = list()
        parents = list()
        for child in sorted_app_schema:
            if is_child(parent, child):
                childrens.append(child)
        if childrens:
            relationships[parent] = {}
            relationships[parent]['childrens'] = childrens
            relationships[parent]['parent'] = get_parent(relationships, parent)

    return relationships

def cleanup_relationships(relationships):

    for parent in relationships:
        if relationships[parent]['parent']:
            for child in relationships[parent]['childrens']:
                other_parent = get_parent(relationships, child, [parent])
                if other_parent:
                    relationships[other_parent]['childrens'].remove(child)
    return relationships


def main():
    xml_file = 'agent.xml'
    write_xml2json(xml_file, 'schema.py', 'app_schema')

    from schema import app_schema

    sorted_app_schema = sorted(app_schema, key=lambda x: x.count('/'))
    relationships = cleanup_relationships(generate_relationships(sorted_app_schema))
    formatted_json = json.dumps(relationships, indent=4, sort_keys=True, ensure_ascii=False)
    with open('schema.py', 'a') as f:
        f.write('relationships = ' + formatted_json)
        f.write('namespace =' + get_xml_namespace(xml_file))
    f.close()


if __name__ == '__main__':
    main()
