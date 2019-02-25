import re
import xml.etree.ElementTree as ET
import json
from collections import Counter


def remove_xml_ns(xml):
    return re.sub(r'{.+?}', '', xml)


def get_xml_ns(xml):
    return re.match(r'{.+?}', xml)


def get_xpath(paths, element=None, attr=None):
    leafs = [path for path in paths]
    if element and attr:
        leafs.append(element)
    leaf = element if element and not attr else '@' + attr

    if leaf in leafs:
        leafs = leafs[:len(leafs) - (len(leafs) - paths.index(leaf)) + 1]
    else:
        leafs.append(leaf)

    return ('/' + ('/').join(leafs))


def update_xpaths(paths, element):
    if not paths:
        paths.append(element)
    elif element not in paths:
        paths.append(element)
    else:
        paths = paths[:len(paths) - (len(paths) - paths.index(element)) + 1]

    return (paths)


def xml_tree_occurence(tree):
    def internal_iter(tree):
        count = Counter(child.tag for child in tree)
        for k, v in count.items():
            k = remove_xml_ns(k)
            if k in accum and accum[k] > v:
                v = accum[k]
            if v > 1:
                accum[k] = v

        for child in tree.getchildren():
            internal_iter(child)

        return accum
    accum = {}
    return internal_iter(tree)


def get_xml_key(xpath, tree_occurence):
    elements = xpath.split('/')
    while len(elements) > 0:
        element = elements.pop()
        if element in tree_occurence:
            return '{}/{}'.format('/'.join(elements), element)


def get_xml_name(key_xpath, xpath):
    key = key_xpath.split('/')[-1]
    parent = xpath.split('/')[-2:-1][0]
    child = xpath.split('/')[-1].replace('@', '')
    return parent + '_' + child if key != parent else child


def xml2dict_schema(element_tree):
    def internal_iter(tree, paths):
        for child in tree.getchildren():
            tree_tag = remove_xml_ns(tree.tag)
            child_tag = remove_xml_ns(child.tag)
            paths = update_xpaths(paths, tree_tag)
            child_xpath = get_xpath(paths, element=child_tag)
            key_xpath = get_xml_key(child_xpath, tree_occurence)
            if not key_xpath:
                key_xpath = get_xpath(paths, element=tree_tag)
                tree_occurence[tree_tag] = "1"
            if key_xpath not in accum:
                accum[key_xpath] = {}

            #print (tree_occurence)
            # print(f' {len(child.getchildren())} and Tree = {tree_tag} Child = {child_tag} ({key_xpath})]')

            if len(child.getchildren()) == 0:
                element = get_xml_name(key_xpath, child_xpath)
                ''' child_xpath is element '''
                if child_xpath not in accum[key_xpath]:
                    accum[key_xpath][child_xpath] = element

            for attr in child.attrib.keys():
                attr_xpath = get_xpath(paths, element=child_tag, attr=attr)
                if child.tag in tree_occurence:
                    attr_tag = remove_xml_ns(attr)
                else:
                    attr_tag = get_xml_name(key_xpath, attr_xpath)
                if attr_xpath not in accum:
                    accum[key_xpath][attr_xpath] = attr_tag

            internal_iter(child, paths)

        return accum

    accum = {}
    tree_occurence = xml_tree_occurence(element_tree)

    return internal_iter(element_tree, [])


def xml2json(xml_file, *args, **kwargs):
    with open(xml_file, 'r') as f:
        return json.dumps(xml2dict_schema(ET.fromstring(f.read())), indent=4, sort_keys=True, ensure_ascii=False)


def write_xml2json(xml_file, json_file, variable_name, *args, **kwargs):
    with open(json_file, 'w') as f:
        f.write(variable_name + '=' + xml2json(xml_file))
    f.close()


def get_xml_namespace(xml_file):
    with open(xml_file, 'r') as f:
        xml_data = ET.fromstring(f.read())
    f.close()
    return get_xml_ns(xml_data.tag)
