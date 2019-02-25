#!/usr/bin/python

import xml.etree.ElementTree as ET
# from xml.etree.ElementTree import XMLID
from schema import app_schema
import json


with open('agent.xml', 'r') as f:
    data = ET.fromstring(f.read())
    # tree, id_map = ET.XMLID(f.read())

# tree, id_map = XMLID(data)
# print('{} = {}'.format(tree,id_map))

ns = {'a': 'http://ACORD.org/Standards/Life/2'}
# child_key = './a:TXLifeRequest/a:OLifE/a:Party'
# child_key = './a:TXLifeRequest/a:OLifE/a:Holding/a:Policy/a:RequirementInfo'
# child_key = './a:TXLifeRequest/a:OLifE/a:Party[4]/a:Address'
# child_key = '.'

# # child_key = './TXLifeRequest/OLifE/Holding/[@id]'
# child_key = './a:TXLifeRequest/a:OLifE/a:Holding'
child_key= './{http://ACORD.org/Standards/Life/2}TXLifeRequest/{http://ACORD.org/Standards/Life/2}OLifE/{http://ACORD.org/Standards/Life/2}Holding'
child_key = "./a:TXLifeRequest/a:OLifE/a:Holding/a:Policy/a:RequirementInfo/a:Attachment"
child_key = ".//a:TXLifeRequest/a:OLifE/a:Holding/a:Policy/a:RequirementInfo[1]/a:Attachment"
parent_key = "./a:TXLifeRequest/a:OLifE/a:Holding/a:Policy/a:RequirementInfo"
parent_key = "./a:TXLifeRequest/a:OLifE/a:Party[0]/a:Address"
# child_key = "./a:Attachment"
child_key = "./a:Priority"
child_key="./{http://ACORD.org/Standards/Life/2}ReqCode"
child_key="./{http://ACORD.org/Standards/Life/2}Attachment/{http://ACORD.org/Standards/Life/2}AttachmentBasicType"
child_key="."
# child_key = "./a:Attachment/a:AttachmentBasicType"
# child_key = "./a:ReqCode"
# child_key = "id"
# child_key = "./a:Policy/a:RequirementInfo"
# # child_key = './acord:TXLifeRequest/acord:OLifE/acord:Holding/acord:Attachment/acord:AttachmentData'
# # child_key = './acord:TXLifeRequest/acord:OLifE/acord:Holding'
# # child_key = './a:TXLifeRequest/a:OLifE/a:Party'
# # child_key = './acord:TXLifeRequest/acord:OLifE'
# row_num = 0
# print((data.findall(parent_key, namespaces=ns)))
# print((data.find(child_key, namespaces=ns).attrib['id']))
# print(data.findall(child_key, namespaces=ns))
# print(data.findall(child_key, namespaces=ns))
# print((data.findall(child_key, namespaces=ns)[0]))
# print(data.findall(child_key, namespaces=ns)[2].tag//*)
for p in data.findall(parent_key, namespaces=ns):
    try:
        # print(p.find(child_key, namespaces=ns).text)
        # print(p.get(child_key, namespaces=ns))
        a=(p.find(child_key, namespaces=ns))
        print(a.get('id'))
        # print(a.text)
    except:
        print('None')
    # print((p.findall(child_key, namespaces=ns)[0].attrib['id']))


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


# sorted_app_schema = sorted(app_schema, key=lambda x: x.count('/'))
# # print(sorted_app_schema)

# relationships = dict()
# for parent in sorted_app_schema:
#     childrens = list()
#     parents = list()
#     for child in sorted_app_schema:
#         if is_child(parent, child):
#             childrens.append(child)
#     if childrens:
#         relationships[parent] = {}
#         relationships[parent]['childrens'] = childrens
#         relationships[parent]['parent'] = get_parent(relationships, parent)


# for parent in relationships:
#     if relationships[parent]['parent']:
#         for child in relationships[parent]['childrens']:
#             other_parent = get_parent(relationships, child, [parent])
#             if other_parent:
#                 relationships[other_parent]['childrens'].remove(child)


# print(json.dumps(relationships, indent=4, sort_keys=True, ensure_ascii=False))
