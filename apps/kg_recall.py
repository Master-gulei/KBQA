# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei

import re
import json
import requests
from itertools import product, chain
from collections import defaultdict
from pypinyin import pinyin, Style
from apps.utils import *


class KGRecall:
    def __init__(
        self, db, space, jieba, log, stop_words_list, tree_dict, get_permission_config
    ):
        """
        初始化
        :param db: nebula对象
        :param space: 图空间
        :param jieba: jieba分词器
        :param log: 日志func
        :param stop_words_list: 停用词库
        :param tree_dict: 改造的trie树-模糊词汇查询算法，包含部门和岗位
        """
        self.log = log
        (
            self.permission_service_url,
            self.permission_timeout,
        ) = get_permission_config()
        self.permission_flag = False
        self.permission = False
        self.related_flag = False
        self.log.info(f"permission_service_url:{self.permission_service_url}")
        self.session = db.session
        self.space = space
        self.session.execute(f"USE {self.space}")
        self.jieba = jieba
        self.log = log
        self.stop_words_list = stop_words_list
        self.tree_dict = tree_dict
        self.user_code = ""  # 格式化后的user_code
        self.ori_user_code = ""  # 格式化后的user_code
        self.entity_class_name_dict = {
            "Employee": "员工",
            "Department": "部门",
            "Organization": "机构",
            "Indicator": "指标",
            "Root": "根因",
            "Menu": "菜单",
            "Position": "岗位",
            "Code": "编号",
            "YtoBehavior": "领域行为",
            "Behavior": "一般行为",
            "ConceptOrganization": "机构概念",
            "OrganizationType": "机构类型",
            "Standard": "制度、政策、标准类",
            "Forms": "报表、账目、合同类",
            "Payment": "费用款项",
            "Statistics": "统计类",
            "Device": "设施、设备、配件类",
            "Barcode": "条码类",
            "Role": "角色、岗位类",
            "Group": "组织、集合类",
            "WebApp": "web、app类",
            "Develop": "开发类",
            "State": "状态",
            "Mode": "模式、方式类",
            "YTO": "圆通",
            "Finance": "金融、企业、学科类",
            "ExternalInternal": "外在或内在形象、标志、属性类",
            "Measurement": "计量、单位类",
            "Part": "部分类",
            "Negation": "否定类",
            "Goods": "产品、商品、货物类",
            "OrderType": "单号类型",
            "ConceptAddress": "地址概念",
            "ConceptPosition": "相对位置概念",
            "Special": "特殊值",
            "ConceptTime": "时间概念",
            "Service": "服务",
            "Project": "项目",
            "TimeDate": "日期时间",
            "ConceptEntity": "实体概念名称",
            "BillList": "票据、清单类",
            "Reference": "区分类",
            "Number": "数量类",
            "Channel": "渠道",
            "Compare": "比较类",
            "Resources": "资源类",
            "Brand": "品牌",
            "Description": "描述、说明类",
            "ConceptCustomer": "客户类",
            "Event": "事件",
            "Label": "标志类",
            "ConceptStore": "店铺类",
            "Rights": "权益",
            "Ability": "能力",
            "Color": "颜色",
            "Province": "省",
            "Other": "其他",
        }
        # 一等必需，二等0.9分，三等0.4分，四等0.15分
        self.first_line_entity_class = [
            "YtoBehavior",
            "Standard",
            "OrganizationType",
            "ConceptOrganization",
            "Forms",
            "Payment",
            "Statistics",
            "Indicator",
            "Menu",
            "Device",
            "Barcode",
            "Role",
            "Group",
            "WebApp",
            "Develop",
            "YTO",
            "Finance",
            "Goods",
            "OrderType",
            "ConceptAddress",
            "Service",
            "Project",
            "BillList",
            "Department",
            "Channel",
            "Resources",
            "Brand",
            "ConceptCustomer",
            "Event",
            "ConceptStore",
            "Province",
        ]
        self.second_line_entity_class = [
            "ConceptTime",
            "State",
            "Part",
            "Negation",
            "TimeDate",
            "Special",
            "Reference",
            "Number",
            "ExternalInternal",
            "Ability",
            "Rights",
        ]
        self.third_line_entity_class = [
            "Behavior",
            "Mode",
            "Measurement",
            "ConceptPosition",
            "ConceptEntity",
            "Compare",
            "Label",
            "Color",
        ]
        self.fourth_line_entity_class = [
            "OrganizationType",
            "OrganizationType",
            "OrganizationType",
            "OrganizationType",
            "OrganizationType",
        ]
        self.entity_class_with_attr_list = [
            "Employee",
            "Department",
            "Organization",
            "Indicator",
            "Menu",
            "Channel",
            "Root",
            "YtoBehavior",
            "Standard",
            "Device",
            "ConceptOrganization",
            "Barcode",
            "WebApp",
            "YTO",
            "Goods",
            "OrderType",
            "ConceptAddress",
            "Payment",
            "Service",
            "ConceptCustomer",
            "Project",
            "ConceptStore",
        ]
        self.order_score_dict = {1: 2, 2: 0.9, 3: 0.4, 4: 0.15}
        self.main_entity_list = [
            "Employee",
            "Department",
            "Organization",
            "Indicator",
            "Menu",
            "Goods",
            "WebApp",
            "Standard",
        ]
        self.edge_class_name_dict = {"attributedOf": "属性"}
        self.employee_public_attribute_list = [
            "员工代码",
            "性别",
            "工作岗位",
            "入职日期",
            "工作职责",
            "所属机构",
            "所属部门",
            "所属直属上级部门",
            "邮箱",
            "员工姓名",
            "直属领导",
            "司龄",
        ]
        self.employee_card_attribute_list = [
            "员工姓名",
            "员工代码",
            "入职日期",
            "工作岗位",
            "工作职责",
            "所属部门",
            "所属机构",
        ]
        self.employee_default_attribute_list = [
            "员工姓名",
            "性别",
            "直属领导",
            "员工代码",
            "入职日期",
            "工作岗位",
            "岗位条线名称",
            "工作职责",
            "所属部门",
            "所属机构",
            "司龄",
        ]
        self.organization_default_attribute_list = ["机构名称", "地址", "所属类型", "负责人"]
        self.indicator_value_inner_name_dict = {"intro": "指标描述"}
        self.entity_class_attr_class_dict = {
            "Employee": [
                "Code",
                "English",
                "Number",
                "Age",
                "Sex",
                "Country",
                "Province",
                "Address",
                "Nation",
                "Position",
                "Assert",
                "Politics",
                "Education",
                "Level",
                "TimeDate",
                "Statement",
                "EmployeeType",
                "Phone",
                "Major",
                "DrivingLicence",
                "Religion",
                "Skill",
                "School",
                "SchoolType",
                "Label",
                "EducationType",
                "Award",
            ],
            "Organization": ["Code", "OrganizationType", "Address", "Statement"],
            "Department": ["Code", "Statement"],
        }
        self.punc_list = [
            ",",
            "，",
            "、",
            "。",
            "-",
            "》",
            "《",
            "(",
            ")",
            "(",
            "\n",
            "\t",
            "\s",
            " ",
            "?",
            "？",
            "!",
            "！",
            "{",
            "}",
            "[",
            "【",
            "]",
            "】",
            "《",
            "》",
            "-",
            ":",
            "：",
            "；",
            ";",
            ".",
            "。",
            "/",
        ]
        self.punc_list = list(set(self.punc_list))
        self.employee_permission_attribute_list = [
            "年龄",
            "身高",
            "体重",
            "国籍",
            "民族",
            "户籍地址",
            "所在省份",
            "是否城市户口",
            "是否已婚",
            "是否有驾驶证",
            "最高学历",
            "出生日期",
            "员工类型",
            "职务级别",
            "政治面貌",
            "工作经历",
            "教育经历",
            "特长",
            "结婚时间",
            "居住地址",
            "是否返聘",
            "最高学历专业",
            "驾驶证",
            "宗教信仰",
            "计算机水平",
            "是否退伍军人",
            "工作经历信息",
            "教育经历信息",
            "奖励信息",
            "人才考察信息",
            "任职信息",
            "绩效信息",
        ]
        self.employee_permission_attribute_class_list = [
            "Number",
            "Age",
            "Country",
            "Nation",
            "Assert",
            "Politics",
            "Education",
            "Level",
            "EmployeeType",
            "Major",
            "DrivingLicence",
            "Religion",
            "Skill",
            "School",
            "SchoolType",
            "EducationType",
            "Label",
            "Award",
        ]
        self.employee_permission_attribute_class_name_dict = {
            "Number": ["身高", "体重"],
            "Age": ["年龄"],
            "Country": ["国籍", "工作经历信息", "教育经历信息"],
            "Nation": ["民族"],
            "Assert": [
                "是否城市户口",
                "是否已婚",
                "是否有快递证",
                "是否有驾驶证",
                "是否返聘",
                "是否退伍军人",
                "是否有留学经历",
            ],
            "Politics": ["政治面貌"],
            "Education": ["教育经历信息", "最高学历"],
            "Level": ["职务级别"],
            "EmployeeType": ["员工类型"],
            "Major": ["最高学历专业", "相关专业名称"],
            "DrivingLicence": ["是否有驾驶证", "驾驶证"],
            "Religion": ["宗教信仰"],
            "Skill": ["计算机水平"],
            "School": ["相关学校标签", "相关学校名称"],
            "SchoolType": ["相关学校标签", "相关学校名称"],
            "EducationType": ["相关学制", "相关学校名称"],
            "Label": ["相关学校标签", "相关学校名称"],
            "Award": ["相关奖励名称"],
        }
        self.employee_permission_attribute_class_name_list = [
            "Age",
            "Nation",
            "Politics",
            "EmployeeType",
            "DrivingLicence",
            "Religion",
            "School",
            "SchoolType",
            "EducationType",
            "Award",
        ]
        self.employee_permission_key_attribute_dict = {
            "教育": ["教育经历信息"],
            "留学": ["教育经历信息", "是否有留学经历"],
            "留过学": ["教育经历信息", "是否有留学经历"],
            "上学": ["教育经历信息"],
            "学制": ["教育经历信息"],
            "婚": ["是否已婚", "结婚时间"],
            "户口": ["是否城市户口"],
            "快递证": ["是否有快递证"],
            "学历": ["最高学历", "教育经历信息"],
            "专业": ["最高学历专业", "教育经历信息"],
            "学校": ["教育经历信息"],
            "资格证": ["资格证"],
            "驾驶证": ["驾驶证", "是否有驾驶证"],
            "户籍": ["所在省份", "户籍地址"],
            "籍贯": ["所在省份", "户籍地址"],
            "级别": ["职务级别"],
            "职级": ["职务级别"],
            "等级": ["职务级别"],
            "政治面貌": ["政治面貌"],
            "国家": ["国籍", "教育经历", "工作经历信息"],
            "特长": ["特长"],
            "兴趣": ["特长"],
            "居住": ["居住地址", "所在省份"],
            "住在": ["居住地址", "所在省份"],
            "住址": ["居住地址", "所在省份"],
            "返聘": ["是否返聘"],
            "再聘": ["是否返聘"],
            "复聘": ["是否返聘"],
            "宗教": ["宗教信仰"],
            "什么教": ["宗教信仰"],
            "计算机": ["计算机水平"],
            "电脑": ["计算机水平"],
            "退伍": ["是否退伍军人"],
            "当兵": ["是否退伍军人"],
            "军人": ["是否退伍军人"],
            "当过兵": ["是否退伍军人"],
            "参军": ["是否退伍军人"],
            "参过军": ["是否退伍军人"],
            "技术能力": ["司龄", "最高学历", "最高学历专业", "计算机水平", "职务级别", "奖励信息", "绩效信息", "人才考察信息"],
            "技术水平": ["司龄", "最高学历", "最高学历专业", "计算机水平", "职务级别", "奖励信息", "绩效信息", "人才考察信息"],
            "技术": ["最高学历", "最高学历专业", "计算机水平", "奖励信息", "绩效信息", "人才考察信息"],
            "工作经历": ["工作经历信息", "司龄", "否有海外工作经历"],
            "就业": ["工作经历信息"],
            "职业": ["工作经历信息"],
            "奖励": ["奖励信息"],
            "激励": ["奖励信息"],
            "获奖": ["奖励信息"],
            "荣誉": ["奖励信息"],
            "考察": ["人才考察信息"],
            "惩罚": ["惩罚信息"],
            "奖惩": ["奖励信息", "惩罚信息"],
            "绩效": ["绩效信息"],
        }

        self.employee_assert_key_attribute_dict = {
            "留学": "是否有留学经历",
            "留过学": "是否有留学经历",
            "海外学习": "是否有留学经历",
            "国外学习": "是否有留学经历",
            "外国学习": "是否有留学经历",
            "海外工作": "是否有留学经历",
            "国外工作": "是否有留学经历",
            "外国工作": "是否有留学经历",
            "婚": "是否已婚",
            "返聘": "是否返聘",
            "再聘": "是否返聘",
            "复聘": "是否返聘",
            "退伍": "是否退伍军人",
            "当兵": "是否退伍军人",
            "军人": "是否退伍军人",
            "当过兵": "是否退伍军人",
            "参军": "是否退伍军人",
            "参过军": "是否退伍军人",
            "驾驶证": "是否有驾驶证",
            "快递证": "是否有快递证",
            "户口": "是否城市户口",
        }
        self.employee_order_attr_dict = {
            "Education": ("最高学历", ["初中", "高中", "中专", "大专", "本科", "硕士", "博士"]),
            "Skill": ("计算机水平", ["较差", "一般", "良好", "熟练", "精通"]),
            "Label": ("相关学校标签", ["一般院校", "211", "双一流", "985"]),
            "Level": (
                "职务级别",
                [
                    "P1",
                    "P2",
                    "P3",
                    "P4",
                    "P5",
                    "P6",
                    "P7",
                    "P8",
                    "P9",
                    "M4",
                    "M5",
                    "M6",
                    "M7",
                    "M8",
                    "M9",
                    "M10",
                    "M11",
                    "M12",
                ],
            ),
        }
        self.compare_word_logic_dict = {
            "大于": ">",
            "小于": "<",
            "不低于": ">=",
            "不高于": "<=",
            "等于": "==",
            "恒等": "==",
            "以上": ">",
            "以下": "<",
            "及以上": ">=",
            "及以下": "<=",
            "超过": ">",
            "高于": ">",
            "低于": "<",
            "不超过": "<=",
            "不大于": "<=",
            "不小于": ">=",
            "之间": "|",
            "区间": "|",
            "介于": "|",
        }
        self.employee_compare_key_attr_dict = {
            "高": "身高",
            "米": "身高",
            "重": "体重",
            "斤": "体重",
            "年龄": "年龄",
            "年纪": "年龄",
            "岁": "年龄",
            "职级": "职务级别",
            "级别": "职务级别",
            "年限": "司龄",
            "年": "司龄",
        }
        self.employee_compare_attr_class_list = [
            "Age",
            "Number",
            "Education",
            "Skill",
            "Label",
            "Level",
        ]
        self.employee_compare_attr_logic_dict = {}
        self.employee_compare_attr_range_dict = {}
        self.employee_compare_attr_value_dict = {}

        self.employee_assert_attr_value_dict = {}

        self.employee_related_permission_attribute_list = []
        self.department_public_attribute_list = []
        self.department_permission_attribute_list = []
        self.organization_public_attribute_list = []
        self.organization_permission_attribute_list = []
        self.legal_entity_transfer_pair_dict = {
            "Employee": ["Employee", "Department", "Organization"],
            "Department": ["Employee", "Department", "Organization"],
            "Organization": ["Employee", "Department", "Organization"],
        }
        self.inner_department_list = [
            "财务部",
            "市场部",
            "办公室",
            "客服部",
            "操作部",
            "综合部",
        ]  # todo 数量很多的部门，查询的时候需要特殊处理
        self.inner_organization_list = ["总公司", "国内事业部"]  # todo 人和部门数量很多的机构，查询的时候需要特殊处理
        self.special_position_word_list = [
            "部长",
            "总裁",
            "省区总经理",
            "省区副总经理",
        ]  # todo 大机构找人，岗位提前，用来提速
        self.special_organization_dict = {"信息科技公司": ["Organization", "国内事业部"]}
        self.need_check_entity_list = ["计算机", "没有", "否"]
        self.entity_need_same_set = set()

    def get_employee_permission(self):
        self.permission_flag = True
        result = False
        params = {"userCode": self.ori_user_code}
        try:
            response = requests.get(
                url=self.permission_service_url,
                params=params,
                timeout=self.permission_timeout,
            )
            data = response.json()
            if data:
                try:
                    result = data["data"]
                except Exception as e:
                    self.log.info(f"permission_service_url parse error: {e}")
        except Exception as e:
            self.log.info(f"call permission_service_url error: {e}")
        self.permission = result

    def get_range_by_logic(self, item_list: list, value, logic):
        value_index = item_list.index(value)
        legal_index_list = [
            item
            for item in range(len(item_list))
            if eval(str(item) + logic + str(value_index))
        ]
        legal_value_list = [item_list[item] for item in legal_index_list]
        return legal_value_list

    def get_entity_count_sql_by_ind(self, ind: str, target_entity: str) -> str:
        sql = (
            ' with [%s] as ot return distinct "99" as a, count(ot) as b, null as c,'
            " null as d,null as e,null as f, null as g " % (ind)
        )
        return sql

    def get_employee_attr_sql_by_ind(self, ind: str, public=False) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        emp_public_str = ""
        if self.permission and self.employee_related_permission_attribute_list:
            self.employee_related_permission_attribute_list = (
                list(set(self.employee_related_permission_attribute_list))
                + self.employee_default_attribute_list
            )
            sql = (
                " match (%s)<-[r1:attributedOf|relatedTo]-(a1) where r1.name in %s "
                ' return distinct "11" as a,[a1,r1,%s] as b,null as c,'
                " null as d,null as e,null as f, null as g limit 200 "
                % (ind, str(self.employee_related_permission_attribute_list), ind)
            )
            return sql

        if public:
            emp_public_str = ',public:"Y"'
        sql = (
            ' match (%s)<-[r1:attributedOf{type:"Employee"%s}]-(a1) where r1.name in %s '
            ' return distinct "11" as a,[a1,r1,%s] as b,null as c,'
            " null as d,null as e,null as f, null as g limit 200 "
            % (ind, emp_public_str, str(self.employee_default_attribute_list), ind)
        )
        return sql

    def get_employee_dep_sql_by_ind(self, ind: str) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        sql = (
            " match (%s)-[r2:belongTo]->(d1:Department{is_valid:'Y'}) where r2.name in ['所属二级上级部门','所属直属上级部门','所属部门'] "
            ' return distinct "12" as a,[%s,r2,d1] as b,null as c,null as d,null as e,null as f, null as g limit 200 '
            % (ind, ind)
        )
        return sql

    def get_employee_org_sql_by_ind(self, ind: str) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        sql = (
            " match (%s)-[r3:belongTo]->(o1:Organization{is_valid:'Y'}) where r3.name in ['所属直属上级机构','所属机构'] "
            ' return distinct "13" as a,[%s,r3,o1] as b, null as c,'
            " null as d,null as e,null as f, null as g limit 200 " % (ind, ind)
        )
        return sql

    def get_employee_all_sql_by_ind(self, ind: str, public=False) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        emp_public_str = ""
        if self.permission and self.employee_related_permission_attribute_list:
            self.employee_related_permission_attribute_list = (
                list(set(self.employee_related_permission_attribute_list))
                + self.employee_default_attribute_list
            )
            sql = (
                " match (%s)<-[r1:attributedOf|relatedTo]-(a1) where r1.name in %s "
                " match (%s)-[r2:belongTo]->(d1:Department{is_valid:'Y'}) where r2.name in ['所属二级上级部门','所属直属上级部门','所属部门'] "
                " match (%s)-[r3:belongTo]->(o1:Organization{is_valid:'Y'}) where r3.name in ['所属直属上级机构','所属机构'] "
                ' return distinct "11" as a,[a1,r1,%s] as b,[%s,r2,d1] as c,'
                " [%s,r3,o1] as d,null as e,null as f, null as g limit 200 "
                % (
                    ind,
                    str(self.employee_related_permission_attribute_list),
                    ind,
                    ind,
                    ind,
                    ind,
                    ind,
                )
            )
            return sql

        if public:
            emp_public_str = ',public:"Y"'
        sql = (
            ' match (%s)<-[r1:attributedOf{type:"Employee"%s}]-(a1) where r1.name in %s '
            " match (%s)-[r2:belongTo]->(d1:Department{is_valid:'Y'}) where r2.name in ['所属二级上级部门','所属直属上级部门','所属部门'] "
            " match (%s)-[r3:belongTo]->(o1:Organization{is_valid:'Y'}) where r3.name in ['所属直属上级机构','所属机构'] "
            ' return distinct "11" as a,[a1,r1,%s] as b,[%s,r2,d1] as c,'
            " [%s,r3,o1] as d,null as e,null as f, null as g limit 200 "
            % (
                ind,
                emp_public_str,
                str(self.employee_default_attribute_list),
                ind,
                ind,
                ind,
                ind,
                ind,
            )
        )

        return sql

    def get_department_sql_by_ind(self, ind: str) -> str:
        """
        获取部门信息查询的sql
        :param ind:
        :return:
        """
        sql = (
            ' optional match (%s)-[r6:belongTo]->(d2:Department{is_valid:"Y"})'
            ' optional match (%s)-[r7:belongTo{name:"所属机构"}]->(o1:Organization{is_valid:"Y"})'
            ' optional match (%s)<-[r4:attributedOf{type:"Department"}]-(a2) '
            ' return distinct "21" as a,[a2,r4,%s] as b,[%s,r6,d2] as c,[%s,r7,o1] as d,null as e ,null as f, null as g  limit 200 '
            % (ind, ind, ind, ind, ind, ind)
        )
        return sql

    def get_organization_attr_sql_by_ind(self, ind: str) -> str:
        """
        获取机构信息查询的sql
        :param ind:
        :return:
        """
        # [a32, a32.Link.name, %s] as f
        sql = (
            ' match (%s)<-[r5:attributedOf{type:"Organization"}]-(a3)  where r5.name in %s '
            ' optional match (%s)<-[r51:attributedOf{name:"工位图",type:"Organization"}]-(a31:Material)'
            " optional match (a31)<-[r52:attributedOf]-(a32:Link)"
            ' return distinct "31" as a,null as b,null as c,[a3,r5,%s] as d,'
            " null as e, null as f, null as g limit 200 "
            % (ind, str(self.organization_default_attribute_list), ind, ind)
        )
        return sql

    def get_organization_belong_sql_by_ind(self, ind: str) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        sql = (
            ' match (%s)-[r7:belongTo]->(o2:Organization{is_valid:"Y"})'
            ' return distinct "32" as a,null as b,null as c,null as d,'
            " [%s,r7,o2] as e,null as f, null as g limit 200 " % (ind, ind)
        )
        return sql

    def get_position_sql_by_name(self, position_name: str) -> str:
        """
        获取岗位信息查询的sql
        :param position_name:
        :return:
        """
        sql = (
            ' match (c1:Position{name:"%s"}) '
            ' match (c1)-[r0:attributedOf]->(e1:Employee{is_valid:"Y"})'
            " match (e1)-[r2:belongTo]->(d1:Department{is_valid:'Y'}) where r2.name in ['所属二级上级部门','所属直属上级部门','所属部门'] "
            " match (e1)-[r3:belongTo]->(o1:Organization{is_valid:'Y'}) where r3.name in ['所属直属上级机构','所属机构'] "
            ' optional match (e1)-[r21:belongTo{name:"所属部门"}]->(d11:Department{is_valid:"Y"})'
            ' optional match (e1)-[r31:belongTo{name:"所属机构"}]->(o11:Organization{is_valid:"Y"})'
            ' optional match (d11)<-[r4:attributedOf{type:"Department"}]-(a2)'
            ' optional match (e1)<-[r1:attributedOf{type:"Employee"}]-(a1)'
            ' return distinct "41" as a,[a1,r1,e1] as b,[e1,r2,d1] as c,[e1,r3,o1] as d,'
            " [a2,r4,d11] as e,null as f, null as g  limit 200 " % (position_name)
        )
        return sql

    def get_code_sql_by_name(self, code_name: str) -> str:
        """
        获取编码信息查询的sql
        :param code_name:
        :return:
        """
        sql = (
            ' match (c1:Code{name:"%s"})-[r0:attributedOf]->(e1)'
            " match (e1)<-[r1:attributedOf]-(a1) where r1.name in ['工作岗位','岗位条线名称','性别','入职日期'] "
            ' return distinct "51" as a,[a1,r1,e1] as b,null as c,null as d,'
            " null as e,null as f, null as g  " % (code_name)
        )
        sql += " union "
        sql += (
            ' match (c1:Code{name:"%s"})-[r0:attributedOf]->(e1)'
            " match (e1)-[r3:belongTo]->(d1)"
            ' return distinct "52" as a,null as b,[e1,r3,d1] as c,null as d,'
            " null as e,null as f, null as g  " % (code_name)
        )
        return sql

    def get_keyword_sql_by_name(self, keyword_name: str) -> str:
        """
        获取关键词信息查询的sql
        :param keyword_name:
        :return:
        """
        sql = (
            ' match (c1:Keyword{name:"%s"})'
            " match (c1)-[r0:relatedTo]->(s1:Statement)"
            " match (s1)-[r1:attributedOf]->(e1)"
            " match (e1)<-[r7:attributedOf]-(p1:Position)"
            ' optional match (e1)-[r2:belongTo{name:"所属部门"}]->(d1:Department{is_valid:"Y"})'
            ' optional match (e1)-[r3:belongTo{name:"所属机构"}]->(o1:Organization{is_valid:"Y"})'
            ' return distinct "61" as a,null as b,[s1,r1,e1] as c,[e1,r2,d1] as d,'
            " [e1,r3,o1] as e,[p1,r7,e1] as f, null as g limit 200  " % (keyword_name)
        )
        return sql

    def get_keyword_sql_by_name_list(self, keyword_name_list: list) -> str:
        """
        获取关键词信息查询的sql
        :param keyword_name:
        :return:
        """
        sql = ""
        for i, keyword_name in enumerate(keyword_name_list):
            i += 1
            sql += (
                ' match (c%s:Keyword{name:"%s"})'
                " match (c%s)-[rk%s:relatedTo]->(s1:Statement)"
                % (i, keyword_name, i, i)
            )
        sql += (
            " match (s1)-[r1:attributedOf]->(e1)"
            " match (e1)<-[r7:attributedOf]-(p1:Position)"
            ' optional match (e1)-[r2:belongTo{name:"所属部门"}]->(d1:Department{is_valid:"Y"})'
            ' optional match (e1)-[r3:belongTo{name:"所属机构"}]->(o1:Organization{is_valid:"Y"})'
            ' return distinct "61" as a,null as b,[s1,r1,e1] as c,[e1,r2,d1] as d,'
            " [e1,r3,o1] as e,[p1,r7,e1] as f, null as g  limit 200 "
        )
        return sql

    def get_indicator_sql_by_name(self, indicator_code: str) -> str:
        """
        获取指标信息查询的sql
        :param indicator_code:
        :return:
        """
        sql = ""
        return sql

    def get_indicator_value_sql_by_name(
        self, org_name: str, indicator_name: str
    ) -> str:
        """
        获取员工信息查询的sql
        :param employee_name:
        :return:
        """
        sql = (
            ' optional match (o1:Organization{name:"%s"})-[r1:belongTo]->(o2:Organization{is_valid:"Y"})'
            ' optional match (i1:Indicator{name:"%s"})<-[r2]-(i2:Indicator{is_valid:"Y"})'
            ' return distinct "81" as a,[o1,r1,o2] as b,[i2,r2,i1] as c,null as d,'
            " null as e,null as f, null as g " % (org_name, indicator_name)
        )
        return sql

    def get_path_by_entity_class(
        self, s_name, e_name, s_class, e_class, rel_list, depth=3
    ):
        """
        根据实体探查路径
        :param s_name:
        :param e_name:
        :param s_class:
        :param e_class:
        :param rel_list:
        :param depth:
        :return:
        """
        rel_str = ""
        if rel_list:
            rel_str = "|".join(rel_list) + "*0.." + str(depth)
        sql = (
            ' match p=(v1:%s{name:"%s"})-[e:%s]->(v2:%s{name:"%s"})'
            ' return distinct "99" as a,p as b,null as c,null as d,'
            " null as e,null as f, null as g  "
            % (s_class, s_name, rel_str, e_class, e_name)
        )
        return sql

    def add_user_sql(self, sql):
        if self.user_code:
            if sql != " union ":
                sql += " union "
            sql += self.get_code_sql_by_name(self.user_code)
        if sql.startswith(" union "):
            sql = sql[7:]
        return sql

    def get_fuzzy_employee_sql(self, employee_set, org_list, dep_list, pos_list):
        sql = ""
        for employee_name in employee_set:
            if len(employee_name) < 2:
                continue
            sql += " union "
            sql_suf = ""
            # 三个字的名称模糊搜索包含两个正确字的
            if len(employee_name) > 2:
                sql += (
                    ' match (e1:Employee) where e1.Employee.name =~ "%s[^\s]+" or e1.Employee.name =~ "%s[^\s]+%s" or e1.Employee.name =~ "[^\s]+%s"'
                    % (
                        employee_name[:2],
                        employee_name[0],
                        employee_name[-1],
                        employee_name[1:],
                    )
                )
            else:
                sql += (
                    ' match (e1:Employee) where e1.Employee.name =~ "%s[^\s]{1,3}" or e1.Employee.name =~ "[^\s]{1,3}%s"'
                    % (employee_name[0], employee_name[-1])
                )
            sql_suf += " optional match (e1)<-[r1:attributedOf{type:'Employee',public:'Y'}]-(a1) "
            if org_list:
                sql += (
                    '  match (e1)-[r3:belongTo]->(o1:Organization{is_valid:"Y"}) where o1.Organization.name in %s'
                    % (str(org_list))
                )
            else:
                sql_suf += ' optional match (e1)-[r3:belongTo{name:"所属机构"}]->(o1:Organization{is_valid:"Y"})'
            if dep_list:
                sql += (
                    '  match (e1)-[r2:belongTo]->(d1:Department{is_valid:"Y"}) where d1.Department.name in %s'
                    % (str(dep_list))
                )
            else:
                sql_suf += ' optional match (e1)-[r2:belongTo{name:"所属部门"}]->(d1:Department{is_valid:"Y"})'
            if pos_list:
                sql += (
                    '  match (c1:Position)-[r0:attributedOf]->(e1:Employee{is_valid:"Y"}) where c1.Position.name in %s'
                    % (str(pos_list))
                )
            else:
                sql_suf += ' optional match (c1:Position)-[r0:attributedOf]->(e1:Employee{is_valid:"Y"})'
            sql += sql_suf
            sql += (
                ' return distinct "%s" as a,[a1,r1,e1] as b,[c1,r0,e1] as c, [e1,r3,o1] as d,[e1,r2,d1] as e,null as f, null as g  '
                % (employee_name)
            )

            sql += " union "
            sql_suf = ""
            # 根据姓名拼音模糊搜索
            employee_py = "".join(
                [item[0] for item in pinyin(employee_name, style=Style.NORMAL)]
            )
            sql += " match (en1:English{name:'%s'})" % (employee_py)
            sql += " match (en1)-[r1:attributedOf{type:'Employee'}]->(e1) "
            sql_suf += " optional match (e1)<-[r1:attributedOf{type:'Employee',public:'Y'}]-(a1) "
            if org_list:
                sql += (
                    '  match (e1)-[r3:belongTo]->(o1:Organization{is_valid:"Y"}) where o1.Organization.name in %s'
                    % (str(org_list))
                )
            else:
                sql_suf += ' optional match (e1)-[r3:belongTo{name:"所属机构"}]->(o1:Organization{is_valid:"Y"})'
            if dep_list:
                sql += (
                    '  match (e1)-[r2:belongTo]->(d1:Department{is_valid:"Y"}) where d1.Department.name in %s'
                    % (str(dep_list))
                )
            else:
                sql_suf += ' optional match (e1)-[r2:belongTo{name:"所属部门"}]->(d1:Department{is_valid:"Y"})'
            if pos_list:
                sql += (
                    '  match (c1:Position)-[r0:attributedOf]->(e1:Employee{is_valid:"Y"}) where c1.Position.name in %s'
                    % (str(pos_list))
                )
            else:
                sql_suf += ' optional match (c1:Position)-[r0:attributedOf]->(e1:Employee{is_valid:"Y"})'
            sql += sql_suf
            sql += (
                ' return distinct "%s" as a,[a1,r1,e1] as b,[c1,r0,e1] as c, [e1,r3,o1] as d,[e1,r2,d1] as e,null as f, null as g  '
                % (employee_name)
            )

        sql = sql[7:]
        if self.user_code:
            sql += " union "
            sql += self.get_code_sql_by_name(self.user_code)

        return sql

    def cut_text(self, text):
        words_list = []
        result = re.findall(r"[0-9A-Za-z./]{2,}", text)
        if result:
            for item in result:
                words_list.append(item)
        cut_result = set(self.jieba.cut(text))
        for item in cut_result:
            item = item.strip()
            if (not item) or (len(item) < 2) or (item in self.stop_words_list):
                continue
            words_list.append(item)
        new_words_list = list(set(words_list))
        new_words_list.sort(key=words_list.index)
        return new_words_list

    def info_sql_run_and_parse(self, sql, knowledge_dict, sql_entity_name_class_dict):
        return_result = defaultdict()
        spo_set_dict = {
            "a": set(),
            "b": set(),
            "c": set(),
            "d": set(),
            "e": set(),
            "f": set(),
        }
        user_name = ""
        s_new_name_new_code_dict = {}
        s_new_name_name_dict = {}
        s_new_code_class_dict = {}
        s_new_code_pro_dict = {}
        new_code_name_dict = {}
        question_answer_dict = {}
        find_employee_set = set()
        employee_some_info_dict = defaultdict(dict)
        department_parent_info = defaultdict(dict)
        organization_parent_info = defaultdict(dict)
        knowledge_fuzzy_name_dict = defaultdict(set)
        try:
            result = self.session.execute(sql)
        except:
            return return_result

        df = result.as_data_frame()
        for index, row in df.iterrows():
            fuzzy_flag = 0
            a = row.get("a", "")
            if sql_entity_name_class_dict and isinstance(a, str):
                if a in sql_entity_name_class_dict:
                    fuzzy_flag = 1
            for key, value in spo_set_dict.items():
                row_result = row.get(key, None)
                if not row_result:
                    continue
                if isinstance(row_result, str):
                    if a == "99":
                        b = row.get("b", "-1")
                        return_result["count"] = str(b)
                        continue
                res = extract_rel(row_result)
                if not res:
                    continue
                _, s_class, res_s_pro = res[0]
                _, p_class, res_p_pro = res[1]
                _, o_class, res_o_pro = res[2]
                if res_s_pro and res_o_pro and res_p_pro:
                    s_name = res_s_pro.get("name", "")
                    if s_class == "Link":
                        s_name = res_s_pro.get("code", "")
                    s_code = res_s_pro.get("code", "")
                    p_name = res_p_pro.get("name", "")
                    o_name = res_o_pro.get("name", "")
                    o_code = res_o_pro.get("code", "")
                    if (
                        not s_name
                        or not o_name
                        or not p_name
                        or s_name == "null"
                        or o_name == "null"
                        or p_name == "null"
                    ):
                        continue
                    if not p_class or p_class == "other":
                        p_class = "attributedOf"
                    if p_class in ["attributedOf", "relatedTo"]:
                        _ = o_name
                        o_name = s_name
                        s_name = _
                        _ = o_code
                        o_code = s_code
                        s_code = _
                        _ = o_class
                        o_class = s_class
                        s_class = _
                    if s_code == self.user_code:
                        user_name = s_name
                    new_s_code = s_code
                    new_o_code = o_code
                    new_s_name = s_name
                    if s_code:
                        if s_class == "Employee":
                            if len(new_s_code) < 8:
                                new_s_code = "0" * (8 - len(new_s_code)) + new_s_code
                                find_employee_set.add(s_name)
                    if o_code:
                        if o_class == "Employee":
                            if len(new_o_code) < 8:
                                new_o_code = "0" * (8 - len(new_o_code)) + new_o_code
                    if s_class in self.entity_class_name_dict:
                        new_s_name = (
                            self.entity_class_name_dict.get(s_class, "")
                            + f"号码为{new_s_code}的{new_s_name}"
                        )
                        s_new_name_new_code_dict[new_s_name] = new_s_code
                    if new_s_code:
                        s_new_code_class_dict[new_s_code] = s_class
                    if new_s_code:
                        new_code_name_dict[new_s_code] = s_name
                        new_code_name_dict[s_code] = s_name
                    if new_o_code:
                        new_code_name_dict[new_o_code] = o_name
                        new_code_name_dict[o_code] = o_name
                    s_new_name_name_dict[new_s_name] = s_name
                    value.add((new_s_name, p_name, o_name))
                    if p_name == "员工代码":
                        if len(o_name) < 8:
                            o_name = "0" * (8 - len(o_name)) + o_name
                    if new_s_name in knowledge_dict:
                        if p_class not in knowledge_dict[new_s_name]:
                            knowledge_dict[new_s_name][p_class] = set()
                        knowledge_dict[new_s_name][p_class].add((p_name, o_name))
                    else:
                        knowledge_dict[new_s_name][p_class] = set()
                        knowledge_dict[new_s_name][p_class].add((p_name, o_name))
                    if s_class == "Employee":
                        if fuzzy_flag:
                            knowledge_fuzzy_name_dict[a].add(s_name)
                        if p_class == "attributedOf":
                            if p_name in ["工作岗位", "岗位条线名称", "工作职责"]:
                                employee_some_info_dict[new_s_code][p_name] = o_name
                        elif p_class == "belongTo":
                            employee_some_info_dict[new_s_code][p_name] = o_code
                        if p_name == "所属部门":
                            knowledge_dict[new_s_name][p_class].add(
                                ("所属部门代码", new_o_code)
                            )
                        elif p_name == "所属机构":
                            knowledge_dict[new_s_name][p_class].add(
                                ("所属机构代码", new_o_code)
                            )
                    if s_class == "Department":
                        if p_class == "belongTo":
                            department_parent_info[new_s_code][p_name] = o_code
                    if s_class == "Organization":
                        if p_class == "belongTo":
                            organization_parent_info[new_s_code][p_name] = o_code
                    if "attributedOf" not in knowledge_dict[new_s_name]:
                        knowledge_dict[new_s_name]["attributedOf"] = set()
                    if s_class == "Employee":
                        p_name = "姓名"
                    else:
                        p_name = "名称"
                    p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                    knowledge_dict[new_s_name]["attributedOf"].add((p_name, s_name))
                    if s_code:
                        p_name = "代码"
                        p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                        knowledge_dict[new_s_name]["attributedOf"].add(
                            (p_name, new_s_code)
                        )
                    if s_class == "FaqQuestion" and o_class == "FaqAnswer":
                        question_answer_dict[s_name] = o_name
                    s_new_code_pro_dict[new_s_code] = res_s_pro
                    s_new_code_pro_dict[new_o_code] = res_o_pro
                else:
                    continue
        # 修正实体之间多关系的情况：1、补充工作岗位
        new_knowledge_dict = knowledge_dict.copy()
        for s_name, rel_info in knowledge_dict.items():
            for rel, info_set in rel_info.items():
                key_value_dict = defaultdict(list)
                for key, value in info_set:
                    key_value_dict[key].append((key, value))
                if "工作岗位" not in key_value_dict and "岗位条线名称" in key_value_dict:
                    for item in key_value_dict["岗位条线名称"]:
                        new_knowledge_dict[s_name][rel].add(("工作岗位", item[-1]))
        knowledge_dict = new_knowledge_dict
        return_result["user_name"] = user_name
        return_result["s_new_name_new_code_dict"] = s_new_name_new_code_dict
        return_result["s_new_name_name_dict"] = s_new_name_name_dict
        return_result["s_new_code_class_dict"] = s_new_code_class_dict
        return_result["s_new_code_pro_dict"] = s_new_code_pro_dict
        return_result["new_code_name_dict"] = new_code_name_dict
        return_result["find_employee_set"] = find_employee_set
        return_result["employee_some_info_dict"] = employee_some_info_dict
        return_result["department_parent_info"] = department_parent_info
        return_result["organization_parent_info"] = organization_parent_info
        return_result["knowledge_fuzzy_name_dict"] = knowledge_fuzzy_name_dict
        return_result["knowledge_dict"] = knowledge_dict
        return_result["question_answer_dict"] = question_answer_dict
        return return_result

    def get_pair_list(self, entity_list, entity_class, sql_entity_name_class_dict):
        entity_name_type_list = []
        for item in entity_list:
            entity_name_type_list.append((item, entity_class))
            sql_entity_name_class_dict[item] = entity_class
        return entity_name_type_list, sql_entity_name_class_dict

    def make_kbqa_sql(
        self,
        order_entity_class_list,
        entity_class_info_set_dict,
        undone_entity_class_attr_dict,
        input_fuzzy_entity_class_list,
        hop_pair_list,
        intention,
        target_entity="employee",
        emp_fuzzy_flag=False,
    ):
        # 指标查值的sql
        if target_entity == "Indicator" and intention == "value":
            org_list = entity_class_info_set_dict.get("Organization", [])
            indicator_list = entity_class_info_set_dict.get("Indicator", [])
            sql = ""
            if org_list and indicator_list:
                sql += self.get_indicator_value_sql_by_name(
                    org_list[0][0], indicator_list[0][0]
                )
            return " union " + sql

        union_sql_list = []
        entity_info_dict = {}
        union_entity_list = []
        entity_class_entity_list_dict = {}
        keyword_list = []
        direct_add_sql_list = []
        related_str = "|relatedTo" if self.related_flag else ""
        target_entity_func_dict = {
            "Employee": [
                self.get_employee_attr_sql_by_ind,
                self.get_employee_dep_sql_by_ind,
                self.get_employee_org_sql_by_ind,
            ],
            "Organization": [
                self.get_organization_attr_sql_by_ind,
                self.get_organization_belong_sql_by_ind,
            ],
            "Department": [self.get_department_sql_by_ind],
        }
        for entity_class in order_entity_class_list:
            (
                entity_list,
                entity_attr_list,
                entity_attr_info_dict,
            ) = entity_class_info_set_dict[entity_class]
            if not entity_list:
                continue
            if entity_class == "Keyword":
                keyword_list = entity_list
                continue
            entity_class_entity_list_dict[entity_class] = entity_list
            for entity in entity_list:
                entity_info_dict[entity] = (
                    entity_class,
                    entity,
                    entity_attr_info_dict.get(entity, []),
                )
            union_entity_list.append(entity_list)
        entity_product_list = [item for item in product(*union_entity_list) if item]

        for item in entity_product_list:
            ind = 0
            can_delete_sql_dict = defaultdict(list)
            base_sql_list = []
            main_sql_list = []
            product_sql_info_list = []
            bridge_ind = 0
            attr_ind = 0
            bridge_sql_list = []
            last_entity_class = ""
            last_entity_attr_info_list = []
            entity_as_entity_class_dict = {}
            for i, entity in enumerate(item):
                if not entity:
                    continue
                main_sql = ""
                entity_class, entity, entity_attr_info_list = entity_info_dict[entity]
                ind += 1
                last_entity_class = entity_class
                last_entity_attr_info_list = entity_attr_info_list
                entity_as = "kt" + str(ind)
                entity_as_entity_class_dict[entity_as] = (entity_class, entity)
                if i:
                    bridge_ind += 1
                    bridge_r_as = "bt" + str(bridge_ind)
                    s_as = "kt" + str(ind)
                    e_as = "kt" + str(ind - 1)
                    s_entity_class, s_name = entity_as_entity_class_dict.get(s_as)
                    e_entity_class, e_name = entity_as_entity_class_dict.get(e_as)
                    if (
                        s_entity_class == "Organization"
                        and e_entity_class == "Employee"
                        and s_name in self.inner_organization_list
                    ):
                        bridge_sql = (
                            ' match (%s:%s{is_valid:"Y"})-[%s:attributedOf|belongTo%s]-(%s) where %s.name in ["所属机构"] '
                            " with %s as %s, %s as %s, %s as %s "
                            % (
                                s_as,
                                entity_class,
                                bridge_r_as,
                                related_str,
                                e_as,
                                bridge_r_as,
                                s_as,
                                s_as,
                                bridge_r_as,
                                bridge_r_as,
                                e_as,
                                e_as,
                            )
                        )
                    else:
                        bridge_sql = (
                            ' match (%s:%s{is_valid:"Y"})-[%s:attributedOf|belongTo|classifyOf|ledBy%s]-(%s) '
                            " with %s as %s, %s as %s, %s as %s "
                            % (
                                s_as,
                                entity_class,
                                bridge_r_as,
                                related_str,
                                e_as,
                                s_as,
                                s_as,
                                bridge_r_as,
                                bridge_r_as,
                                e_as,
                                e_as,
                            )
                        )
                    if bridge_sql not in bridge_sql_list:
                        bridge_sql_list.append(bridge_sql)
                if entity_class == "Code":
                    main_sql += (
                        ' match (c1:%s{name:"%s"})-[r0:attributedOf%s]->(%s)'
                        % (entity_class, entity, related_str, entity_as)
                    )
                    # todo
                    if target_entity not in ["Employee", "Organization"]:
                        target_entity = "Employee"
                elif entity_class == "Department":
                    # 对于数量很多的部门，找跟提问人最近的，缩减时间
                    if entity in self.inner_department_list:
                        main_sql += (
                            ' match (c99:Code{name:"%s"})-[r0:attributedOf]->(e99) '
                            ' match (e99)-[r99:belongTo]->(o99:%s{is_valid:"Y"}) '
                            ' match (o99)-[r7:belongTo]-(%s:%s{name:"%s"}) '
                            % (
                                self.user_code,
                                entity_class,
                                entity_as,
                                entity_class,
                                entity,
                            )
                        )
                        direct_add_sql = (
                            ' union match (c99:Code{name:"%s"})-[r0:attributedOf]->(e99) '
                            ' match (e99)-[r99:belongTo]->(o99:%s{is_valid:"Y"}) '
                            ' match (o99)-[r7:belongTo]-(%s:%s{name:"%s"}) '
                            ' match (%s)<-[r4:attributedOf{type:"Department"}]-(a2) where r4.name in ["负责人","主管","部门代码"]  '
                            ' return distinct "41" as a,[a2,r4,%s] as b,null as c,null as d,'
                            " null as e,null as f, null as g  limit 200 "
                            % (
                                self.user_code,
                                entity_class,
                                entity_as,
                                entity_class,
                                entity,
                                entity_as,
                                entity_as,
                            )
                        )
                        direct_add_sql_list.append(direct_add_sql)
                    else:
                        main_sql += " match (%s:%s{name:'%s'}) " % (
                            entity_as,
                            entity_class,
                            entity,
                        )
                        direct_add_sql = (
                            " union match (%s:%s{name:'%s'}) "
                            ' match (%s)<-[r4:attributedOf{type:"Department"}]-(a2) where r4.name in ["负责人","主管","部门代码"]  '
                            ' return distinct "41" as a,[a2,r4,%s] as b,null as c,null as d,'
                            " null as e,null as f, null as g  limit 200 "
                            % (entity_as, entity_class, entity, entity_as, entity_as)
                        )
                        direct_add_sql_list.append(direct_add_sql)
                elif entity_class == "Organization":
                    main_sql += " match (%s:%s{name:'%s'}) " % (
                        entity_as,
                        entity_class,
                        entity,
                    )
                    direct_add_sql = (
                        " union match (%s:%s{name:'%s'}) "
                        ' match (%s)<-[r5:attributedOf{type:"Organization"}]-(a3) where r5.name in ["负责人","主管","机构代码"]  '
                        ' return distinct "31" as a,[a3,r5,%s] as b,null as c,null as d,'
                        " null as e,null as f, null as g  limit 200 "
                        % (entity_as, entity_class, entity, entity_as, entity_as)
                    )
                    direct_add_sql_list.append(direct_add_sql)
                else:
                    if entity_class == "Employee" and emp_fuzzy_flag:
                        employee_py = "".join(
                            [item[0] for item in pinyin(entity, style=Style.NORMAL)]
                        )
                        main_sql += " match (en1:English{name:'%s'})" % (employee_py)
                        main_sql += (
                            " match (en1)-[r99:attributedOf{type:'Employee'}]->(%s:%s) "
                            % (entity_as, entity_class)
                        )
                    else:
                        main_sql += " match (%s:%s{name:'%s'}) " % (
                            entity_as,
                            entity_class,
                            entity,
                        )
                fuzzy_attr_entity_info_list = []
                delete_attr_entity_info_list = []
                for bean in entity_attr_info_list:
                    attr_value, attr_key = bean
                    if attr_key in input_fuzzy_entity_class_list:
                        fuzzy_attr_entity_info_list.append(bean)
                        delete_attr_entity_info_list.append(bean)
                for bean in delete_attr_entity_info_list:
                    entity_attr_info_list.remove(bean)
                mid_sql = ""
                assert_flag = False
                for bean in entity_attr_info_list:
                    attr_ind += 1
                    entity_attr_as = "at" + str(attr_ind)
                    entity_r_as = "rt" + str(attr_ind)
                    attr_value, attr_key = bean
                    if attr_key == "Assert":
                        if assert_flag:
                            continue
                        attr_ind -= 1
                        for (
                            r_name,
                            assert_value,
                        ) in self.employee_assert_attr_value_dict.items():
                            attr_ind += 1
                            entity_attr_as = "at" + str(attr_ind)
                            entity_r_as = "rt" + str(attr_ind)
                            mid_sql += (
                                " match (%s:%s{name:'%s'})-[%s:attributedOf%s]->(%s) where %s.name == '%s'"
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    assert_value,
                                    entity_r_as,
                                    related_str,
                                    entity_as,
                                    entity_r_as,
                                    r_name,
                                )
                            )
                        assert_flag = True
                    elif attr_key in self.employee_compare_attr_range_dict:
                        (
                            rel_name,
                            min_item,
                            max_item,
                        ) = self.employee_compare_attr_range_dict[attr_key]
                        mid_sql += (
                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name)> %s and toFloat(%s.%s.name)< %s "
                            % (
                                entity_as,
                                entity_r_as,
                                related_str,
                                entity_attr_as,
                                attr_key,
                                entity_r_as,
                                rel_name,
                                entity_attr_as,
                                attr_key,
                                min_item,
                                entity_attr_as,
                                attr_key,
                                max_item,
                            )
                        )
                    elif attr_key in self.employee_compare_attr_logic_dict:
                        (
                            rel_name,
                            item_value,
                            compare,
                        ) = self.employee_compare_attr_logic_dict[attr_key]
                        mid_sql += (
                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name) %s %s"
                            % (
                                entity_as,
                                entity_r_as,
                                related_str,
                                entity_attr_as,
                                attr_key,
                                entity_r_as,
                                rel_name,
                                entity_attr_as,
                                attr_key,
                                compare,
                                item_value,
                            )
                        )
                    elif attr_key in self.employee_compare_attr_value_dict:
                        rel_name, item_list = self.employee_compare_attr_value_dict[
                            attr_key
                        ]
                        mid_sql += (
                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and %s.%s.name in %s"
                            % (
                                entity_as,
                                entity_r_as,
                                related_str,
                                entity_attr_as,
                                attr_key,
                                entity_r_as,
                                rel_name,
                                entity_attr_as,
                                attr_key,
                                str(item_list),
                            )
                        )
                    else:
                        if attr_value in self.entity_need_same_set:
                            entity_attr_as_add_one = "at" + str(attr_ind + 1)
                            entity_attr_as_add_two = "at" + str(attr_ind + 2)
                            mid_sql += (
                                " match (%s:%s{name:'%s'})  optional match (%s)-[:sameAs]-(%s:%s) unwind [%s,%s] as %s with distinct %s as %s "
                                " match (%s)<-[%s:attributedOf%s]-(%s) "
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    attr_value,
                                    entity_attr_as,
                                    entity_attr_as_add_one,
                                    attr_key,
                                    entity_attr_as,
                                    entity_attr_as_add_one,
                                    entity_attr_as_add_two,
                                    entity_attr_as_add_two,
                                    entity_attr_as_add_two,
                                    entity_as,
                                    entity_r_as,
                                    related_str,
                                    entity_attr_as_add_two,
                                )
                            )
                            attr_ind += 2
                        else:
                            mid_sql += (
                                " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                                % (
                                    entity_as,
                                    entity_r_as,
                                    related_str,
                                    entity_attr_as,
                                    attr_key,
                                    attr_value,
                                )
                            )
                    entity_list = entity_class_info_set_dict[entity_class][0]
                    if target_entity == entity_class or len(entity_list) > 1:
                        can_delete_sql_dict[entity_class].append(mid_sql)
                main_sql += mid_sql
                if main_sql not in main_sql_list:
                    main_sql_list.append(main_sql)
                if fuzzy_attr_entity_info_list:
                    attr_ind += 1
                    for bean in fuzzy_attr_entity_info_list:
                        entity_attr_as = "at" + str(attr_ind)
                        entity_r_as = "rt" + str(attr_ind)
                        attr_value, attr_key = bean
                        mid_sql = (
                            " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                            % (
                                entity_as,
                                entity_r_as,
                                related_str,
                                entity_attr_as,
                                attr_key,
                                attr_value,
                            )
                        )
                        entity_list = entity_class_info_set_dict[entity_class][0]
                        if target_entity == entity_class or len(entity_list) > 1:
                            can_delete_sql_dict[entity_class].append(mid_sql)
                        product_sql_info_list.append(
                            (mid_sql, attr_key, attr_value, ind)
                        )
            sql = " ".join(main_sql_list)

            if product_sql_info_list:
                for bean in product_sql_info_list:
                    mid_sql, attr_key, attr_value, ind = bean
                    special_position_flag = 0
                    if attr_key == "Position":
                        for special_position in self.special_position_word_list:
                            if special_position in attr_value:
                                special_position_flag = 1
                        if special_position_flag:
                            sql = mid_sql + sql
                        else:
                            sql += mid_sql
                    else:
                        sql += mid_sql
                    sql += " ".join(bridge_sql_list)
                    if sql not in base_sql_list:
                        base_sql_list.append((sql, ind))
            else:
                sql += " ".join(bridge_sql_list)
                if sql not in base_sql_list:
                    base_sql_list.append((sql, ind))

            new_base_sql_list = base_sql_list[:]
            if last_entity_class != target_entity and intention == "count":
                new_base_sql_list = []
            bridge_ind += 1
            if intention != "info":
                for bean in base_sql_list:
                    base_sql, ind = bean
                    ind += 1
                    entity_as = "kt" + str(ind)
                    s_as = "kt" + str(ind - 1)
                    bridge_r_as = "bt" + str(bridge_ind)
                    s_entity_class, s_name = entity_as_entity_class_dict.get(s_as)
                    if (
                        s_entity_class == "Organization"
                        and target_entity == "Employee"
                        and s_name in self.inner_organization_list
                    ):
                        base_sql += (
                            ' match (%s)-[%s:attributedOf|belongTo%s]-(%s:%s{is_valid:"Y"}) where %s.name in ["所属机构"]'
                            " with %s as %s, %s as %s, %s as %s "
                            % (
                                s_as,
                                bridge_r_as,
                                related_str,
                                entity_as,
                                target_entity,
                                bridge_r_as,
                                s_as,
                                s_as,
                                bridge_r_as,
                                bridge_r_as,
                                entity_as,
                                entity_as,
                            )
                        )
                    elif (
                        s_entity_class in ["Organization", "Department"]
                        and target_entity == "Employee"
                    ):
                        base_sql += (
                            ' match (%s)-[%s:attributedOf|belongTo%s]-(%s:%s{is_valid:"Y"}) '
                            " with %s as %s, %s as %s, %s as %s "
                            % (
                                s_as,
                                bridge_r_as,
                                related_str,
                                entity_as,
                                target_entity,
                                s_as,
                                s_as,
                                bridge_r_as,
                                bridge_r_as,
                                entity_as,
                                entity_as,
                            )
                        )
                    else:
                        base_sql += (
                            ' match (%s)-[%s:attributedOf|belongTo|classifyOf|ledBy%s]-(%s:%s{is_valid:"Y"}) '
                            " with %s as %s, %s as %s, %s as %s "
                            % (
                                s_as,
                                bridge_r_as,
                                related_str,
                                entity_as,
                                target_entity,
                                s_as,
                                s_as,
                                bridge_r_as,
                                bridge_r_as,
                                entity_as,
                                entity_as,
                            )
                        )
                    entity_class_attr_info_dict = undone_entity_class_attr_dict.get(
                        target_entity, {}
                    )
                    if not can_delete_sql_dict and not entity_class_attr_info_dict:
                        new_base_sql_list.append((base_sql, ind))
                    if target_entity == last_entity_class:
                        can_delete_sql_list = can_delete_sql_dict[target_entity]
                        if last_entity_attr_info_list and can_delete_sql_dict:
                            for delete_sql in can_delete_sql_list:
                                base_sql = base_sql.replace(delete_sql, " ")
                            assert_flag = False
                            mid_sql = ""
                            for bean in last_entity_attr_info_list:
                                attr_ind += 1
                                entity_attr_as = "at" + str(attr_ind)
                                entity_r_as = "rt" + str(attr_ind)
                                attr_value, attr_key = bean
                                if attr_key == "Assert":
                                    if assert_flag:
                                        continue
                                    attr_ind -= 1
                                    for (
                                        r_name,
                                        assert_value,
                                    ) in self.employee_assert_attr_value_dict.items():
                                        attr_ind += 1
                                        entity_attr_as = "at" + str(attr_ind)
                                        entity_r_as = "rt" + str(attr_ind)
                                        mid_sql += (
                                            " match (%s:%s{name:'%s'})-[%s:attributedOf%s]->(%s) where %s.name == '%s'"
                                            % (
                                                entity_attr_as,
                                                attr_key,
                                                assert_value,
                                                entity_r_as,
                                                related_str,
                                                entity_as,
                                                entity_r_as,
                                                r_name,
                                            )
                                        )
                                    assert_flag = True
                                elif attr_key in self.employee_compare_attr_range_dict:
                                    (
                                        rel_name,
                                        min_item,
                                        max_item,
                                    ) = self.employee_compare_attr_range_dict[attr_key]
                                    mid_sql += (
                                        " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name)> %s and toFloat(%s.%s.name)< %s "
                                        % (
                                            entity_as,
                                            entity_r_as,
                                            related_str,
                                            entity_attr_as,
                                            attr_key,
                                            entity_r_as,
                                            rel_name,
                                            entity_attr_as,
                                            attr_key,
                                            min_item,
                                            entity_attr_as,
                                            attr_key,
                                            max_item,
                                        )
                                    )
                                elif attr_key in self.employee_compare_attr_logic_dict:
                                    (
                                        rel_name,
                                        item_value,
                                        compare,
                                    ) = self.employee_compare_attr_logic_dict[attr_key]
                                    mid_sql += (
                                        " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name) %s %s"
                                        % (
                                            entity_as,
                                            entity_r_as,
                                            related_str,
                                            entity_attr_as,
                                            attr_key,
                                            entity_r_as,
                                            rel_name,
                                            entity_attr_as,
                                            attr_key,
                                            compare,
                                            item_value,
                                        )
                                    )
                                elif attr_key in self.employee_compare_attr_value_dict:
                                    (
                                        rel_name,
                                        item_list,
                                    ) = self.employee_compare_attr_value_dict[attr_key]
                                    mid_sql += (
                                        " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and %s.%s.name in %s"
                                        % (
                                            entity_as,
                                            entity_r_as,
                                            related_str,
                                            entity_attr_as,
                                            attr_key,
                                            entity_r_as,
                                            rel_name,
                                            entity_attr_as,
                                            attr_key,
                                            str(item_list),
                                        )
                                    )
                                else:
                                    if attr_value in self.entity_need_same_set:
                                        entity_attr_as_add_one = "at" + str(
                                            attr_ind + 1
                                        )
                                        entity_attr_as_add_two = "at" + str(
                                            attr_ind + 2
                                        )
                                        mid_sql += (
                                            " match (%s:%s{name:'%s'})  optional match (%s)-[:sameAs]-(%s:%s) unwind [%s,%s] as %s with distinct %s as %s "
                                            " match (%s)<-[%s:attributedOf%s]-(%s) "
                                            % (
                                                entity_attr_as,
                                                attr_key,
                                                attr_value,
                                                entity_attr_as,
                                                entity_attr_as_add_one,
                                                attr_key,
                                                entity_attr_as,
                                                entity_attr_as_add_one,
                                                entity_attr_as_add_two,
                                                entity_attr_as_add_two,
                                                entity_attr_as_add_two,
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as_add_two,
                                            )
                                        )
                                        attr_ind += 2
                                    else:
                                        mid_sql += (
                                            " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                                            % (
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as,
                                                attr_key,
                                                attr_value,
                                            )
                                        )
                            base_sql += mid_sql
                            if base_sql not in new_base_sql_list:
                                new_base_sql_list.append((base_sql, ind))
                    else:
                        if entity_class_attr_info_dict:
                            fuzzy_attr_entity_class_list = []
                            delete_attr_entity_class_list = []
                            for (
                                attr_key,
                                attr_value_list,
                            ) in entity_class_attr_info_dict.items():
                                if attr_key in input_fuzzy_entity_class_list:
                                    fuzzy_attr_entity_class_list.append(attr_key)
                                    delete_attr_entity_class_list.append(attr_key)
                            mid_sql = ""
                            assert_flag = False
                            for (
                                attr_key,
                                attr_value_list,
                            ) in entity_class_attr_info_dict.items():
                                if attr_key in input_fuzzy_entity_class_list:
                                    continue
                                for attr_value in attr_value_list:
                                    attr_ind += 1
                                    entity_attr_as = "at" + str(attr_ind)
                                    entity_r_as = "rt" + str(attr_ind)
                                    if attr_key == "Assert":
                                        if assert_flag:
                                            continue
                                        attr_ind -= 1
                                        for (
                                            r_name,
                                            assert_value,
                                        ) in (
                                            self.employee_assert_attr_value_dict.items()
                                        ):
                                            attr_ind += 1
                                            entity_attr_as = "at" + str(attr_ind)
                                            entity_r_as = "rt" + str(attr_ind)
                                            mid_sql += (
                                                " match (%s:%s{name:'%s'})-[%s:attributedOf%s]->(%s) where %s.name == '%s'"
                                                % (
                                                    entity_attr_as,
                                                    attr_key,
                                                    assert_value,
                                                    entity_r_as,
                                                    related_str,
                                                    entity_as,
                                                    entity_r_as,
                                                    r_name,
                                                )
                                            )
                                        assert_flag = True
                                    elif (
                                        attr_key
                                        in self.employee_compare_attr_range_dict
                                    ):
                                        (
                                            rel_name,
                                            min_item,
                                            max_item,
                                        ) = self.employee_compare_attr_range_dict[
                                            attr_key
                                        ]
                                        mid_sql += (
                                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name)> %s and toFloat(%s.%s.name)< %s "
                                            % (
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as,
                                                attr_key,
                                                entity_r_as,
                                                rel_name,
                                                entity_attr_as,
                                                attr_key,
                                                min_item,
                                                entity_attr_as,
                                                attr_key,
                                                max_item,
                                            )
                                        )
                                    elif (
                                        attr_key
                                        in self.employee_compare_attr_logic_dict
                                    ):
                                        (
                                            rel_name,
                                            item_value,
                                            compare,
                                        ) = self.employee_compare_attr_logic_dict[
                                            attr_key
                                        ]
                                        mid_sql += (
                                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name) %s %s"
                                            % (
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as,
                                                attr_key,
                                                entity_r_as,
                                                rel_name,
                                                entity_attr_as,
                                                attr_key,
                                                compare,
                                                item_value,
                                            )
                                        )
                                    elif (
                                        attr_key
                                        in self.employee_compare_attr_value_dict
                                    ):
                                        (
                                            rel_name,
                                            item_list,
                                        ) = self.employee_compare_attr_value_dict[
                                            attr_key
                                        ]
                                        mid_sql += (
                                            " match (%s)<-[%s:attributedOf%s]-(%s:%s) where %s.name == '%s' and %s.%s.name in %s"
                                            % (
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as,
                                                attr_key,
                                                entity_r_as,
                                                rel_name,
                                                entity_attr_as,
                                                attr_key,
                                                str(item_list),
                                            )
                                        )
                                    else:
                                        if attr_value in self.entity_need_same_set:
                                            entity_attr_as_add_one = "at" + str(
                                                attr_ind + 1
                                            )
                                            entity_attr_as_add_two = "at" + str(
                                                attr_ind + 2
                                            )
                                            mid_sql += (
                                                " match (%s:%s{name:'%s'})  optional match (%s)-[:sameAs]-(%s:%s) unwind [%s,%s] as %s with distinct %s as %s "
                                                " match (%s)<-[%s:attributedOf%s]-(%s) "
                                                % (
                                                    entity_attr_as,
                                                    attr_key,
                                                    attr_value,
                                                    entity_attr_as,
                                                    entity_attr_as_add_one,
                                                    attr_key,
                                                    entity_attr_as,
                                                    entity_attr_as_add_one,
                                                    entity_attr_as_add_two,
                                                    entity_attr_as_add_two,
                                                    entity_attr_as_add_two,
                                                    entity_as,
                                                    entity_r_as,
                                                    related_str,
                                                    entity_attr_as_add_two,
                                                )
                                            )
                                            attr_ind += 2
                                        else:
                                            mid_sql += (
                                                " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                                                % (
                                                    entity_as,
                                                    entity_r_as,
                                                    related_str,
                                                    entity_attr_as,
                                                    attr_key,
                                                    attr_value,
                                                )
                                            )
                            base_sql += mid_sql
                            if fuzzy_attr_entity_class_list:
                                attr_ind += 1
                                for (
                                    attr_key,
                                    attr_value_list,
                                ) in entity_class_attr_info_dict.items():
                                    for attr_value in attr_value_list:
                                        entity_attr_as = "at" + str(attr_ind)
                                        entity_r_as = "rt" + str(attr_ind)
                                        mid_sql = (
                                            " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                                            % (
                                                entity_as,
                                                entity_r_as,
                                                related_str,
                                                entity_attr_as,
                                                attr_key,
                                                attr_value,
                                            )
                                        )
                                        special_position_flag = 0
                                        if attr_key == "Position":
                                            for (
                                                special_position
                                            ) in self.special_position_word_list:
                                                if special_position in attr_value:
                                                    special_position_flag = 1
                                            if special_position_flag:
                                                mid_sql = mid_sql + base_sql
                                            else:
                                                mid_sql = base_sql + mid_sql
                                        else:
                                            mid_sql = base_sql + mid_sql
                                        if mid_sql not in new_base_sql_list:
                                            new_base_sql_list.append((mid_sql, ind))
                            elif base_sql not in new_base_sql_list:
                                new_base_sql_list.append((base_sql, ind))

            base_sql_list = new_base_sql_list[:]
            done_bean_list = []
            if intention == "find":
                for bean in base_sql_list:
                    if bean in done_bean_list:
                        continue
                    done_bean_list.append(bean)
                    sql, ind = bean
                    for i, func in enumerate(
                        target_entity_func_dict.get(target_entity, [])
                    ):
                        mid_sql = sql
                        mid_sql += func("kt" + str(ind))
                        union_sql_list.append(mid_sql)
            elif intention == "count":
                for bean in base_sql_list:
                    if bean in done_bean_list:
                        continue
                    done_bean_list.append(bean)
                    sql, ind = bean
                    mid_sql = sql + self.get_entity_count_sql_by_ind(
                        "kt" + str(ind), target_entity
                    )
                    union_sql_list.append(mid_sql)
            else:
                # intention == "info":
                for bean in base_sql_list:
                    if bean in done_bean_list:
                        continue
                    done_bean_list.append(bean)
                    sql, ind = bean
                    for i, func in enumerate(
                        target_entity_func_dict.get(target_entity, [])
                    ):
                        mid_sql = sql
                        mid_sql += func("kt" + str(ind))
                        union_sql_list.append(mid_sql)

        if not entity_product_list:
            new_base_sql_list = []
            entity_class_attr_info_dict = undone_entity_class_attr_dict.get(
                target_entity, {}
            )
            if entity_class_attr_info_dict:
                ind = 1
                attr_ind = 1
                entity_as = "kt" + str(ind)
                if target_entity == "Employee":
                    base_sql = (
                        ' match (o99:Organization{is_valid:"Y", name:"国内事业部"})<-[r3:belongTo{name:"所属机构"}]-(%s:%s) '
                        % (entity_as, target_entity)
                    )
                else:
                    base_sql = ""
                fuzzy_attr_entity_class_list = []
                delete_attr_entity_class_list = []
                for attr_key, attr_value_list in entity_class_attr_info_dict.items():
                    if attr_key in input_fuzzy_entity_class_list:
                        fuzzy_attr_entity_class_list.append(attr_key)
                        delete_attr_entity_class_list.append(attr_key)
                mid_sql = ""
                assert_flag = False
                for attr_key, attr_value_list in entity_class_attr_info_dict.items():
                    if attr_key in input_fuzzy_entity_class_list:
                        continue
                    for attr_value in attr_value_list:
                        attr_ind += 1
                        entity_attr_as = "at" + str(attr_ind)
                        entity_r_as = "rt" + str(attr_ind)
                        if attr_key == "Assert":
                            if assert_flag:
                                continue
                            attr_ind -= 1
                            for (
                                r_name,
                                assert_value,
                            ) in self.employee_assert_attr_value_dict.items():
                                attr_ind += 1
                                entity_attr_as = "at" + str(attr_ind)
                                entity_r_as = "rt" + str(attr_ind)
                                mid_sql += (
                                    " match (%s:%s{name:'%s'})-[%s:attributedOf%s]->(%s) where %s.name == '%s'"
                                    % (
                                        entity_attr_as,
                                        attr_key,
                                        assert_value,
                                        entity_r_as,
                                        related_str,
                                        entity_as,
                                        entity_r_as,
                                        r_name,
                                    )
                                )
                            assert_flag = True
                        elif attr_key in self.employee_compare_attr_range_dict:
                            (
                                rel_name,
                                min_item,
                                max_item,
                            ) = self.employee_compare_attr_range_dict[attr_key]
                            mid_sql += (
                                " match (%s:%s)-[%s:attributedOf%s]->(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name)> %s and toFloat(%s.%s.name)< %s "
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    related_str,
                                    entity_r_as,
                                    entity_as,
                                    target_entity,
                                    entity_r_as,
                                    rel_name,
                                    entity_attr_as,
                                    attr_key,
                                    min_item,
                                    entity_attr_as,
                                    attr_key,
                                    max_item,
                                )
                            )
                        elif attr_key in self.employee_compare_attr_logic_dict:
                            (
                                rel_name,
                                item_value,
                                compare,
                            ) = self.employee_compare_attr_logic_dict[attr_key]
                            mid_sql += (
                                " match (%s:%s)-[%s:attributedOf%s]->(%s:%s) where %s.name == '%s' and toFloat(%s.%s.name) %s %s"
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    entity_r_as,
                                    related_str,
                                    entity_as,
                                    target_entity,
                                    entity_r_as,
                                    rel_name,
                                    entity_attr_as,
                                    attr_key,
                                    compare,
                                    item_value,
                                )
                            )
                        elif attr_key in self.employee_compare_attr_value_dict:
                            rel_name, item_list = self.employee_compare_attr_value_dict[
                                attr_key
                            ]
                            mid_sql += (
                                " match (%s:%s)-[%s:attributedOf%s]->(%s:%s) where %s.name == '%s' and %s.%s.name in %s"
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    entity_r_as,
                                    related_str,
                                    entity_as,
                                    target_entity,
                                    entity_r_as,
                                    rel_name,
                                    entity_attr_as,
                                    attr_key,
                                    str(item_list),
                                )
                            )
                        else:
                            if attr_value in self.entity_need_same_set:
                                entity_attr_as_add_one = "at" + str(attr_ind + 1)
                                entity_attr_as_add_two = "at" + str(attr_ind + 2)
                                mid_sql += (
                                    " match (%s:%s{name:'%s'})  optional match (%s)-[:sameAs]-(%s:%s) unwind [%s,%s] as %s with distinct %s as %s "
                                    " match (%s)<-[%s:attributedOf%s]-(%s) "
                                    % (
                                        entity_attr_as,
                                        attr_key,
                                        attr_value,
                                        entity_attr_as,
                                        entity_attr_as_add_one,
                                        attr_key,
                                        entity_attr_as,
                                        entity_attr_as_add_one,
                                        entity_attr_as_add_two,
                                        entity_attr_as_add_two,
                                        entity_attr_as_add_two,
                                        entity_as,
                                        entity_r_as,
                                        related_str,
                                        entity_attr_as_add_two,
                                    )
                                )
                                attr_ind += 2
                            else:
                                mid_sql += (
                                    " match (%s)<-[%s:attributedOf%s]-(%s:%s{name:'%s'}) "
                                    % (
                                        entity_as,
                                        entity_r_as,
                                        related_str,
                                        entity_attr_as,
                                        attr_key,
                                        attr_value,
                                    )
                                )
                base_sql += mid_sql
                if fuzzy_attr_entity_class_list:
                    attr_ind += 1
                    for (
                        attr_key,
                        attr_value_list,
                    ) in entity_class_attr_info_dict.items():
                        for attr_value in attr_value_list:
                            entity_attr_as = "at" + str(attr_ind)
                            entity_r_as = "rt" + str(attr_ind)
                            mid_sql += (
                                " match (%s:%s{name:'%s'})-[%s:attributedOf%s]->(%s)"
                                % (
                                    entity_attr_as,
                                    attr_key,
                                    attr_value,
                                    entity_r_as,
                                    related_str,
                                    entity_as,
                                )
                            )
                            special_position_flag = 0
                            if attr_key == "Position":
                                for special_position in self.special_position_word_list:
                                    if special_position in attr_value:
                                        special_position_flag = 1
                                if special_position_flag:
                                    mid_sql = mid_sql + base_sql
                                else:
                                    mid_sql = base_sql + mid_sql
                            else:
                                mid_sql = base_sql + mid_sql
                            if mid_sql not in new_base_sql_list:
                                new_base_sql_list.append((mid_sql, ind))
                elif base_sql not in new_base_sql_list:
                    new_base_sql_list.append((base_sql, ind))
            base_sql_list = new_base_sql_list[:]
            done_bean_list = []
            if intention == "count":
                for bean in base_sql_list:
                    if bean in done_bean_list:
                        continue
                    done_bean_list.append(bean)
                    sql, ind = bean
                    mid_sql = sql + self.get_entity_count_sql_by_ind(
                        "kt" + str(ind), target_entity
                    )
                    union_sql_list.append(mid_sql)
            else:
                for bean in base_sql_list:
                    if bean in done_bean_list:
                        continue
                    done_bean_list.append(bean)
                    sql, ind = bean
                    # 找人的场景把属性、部门、机构整合到一个sql中执行
                    if target_entity == "Employee" and not entity_product_list:
                        mid_sql = sql
                        mid_sql += self.get_employee_all_sql_by_ind("kt" + str(ind))
                        union_sql_list.append(mid_sql)
                    else:
                        for i, func in enumerate(
                            target_entity_func_dict.get(target_entity, [])
                        ):
                            mid_sql = sql
                            mid_sql += func("kt" + str(ind))
                            union_sql_list.append(mid_sql)

        if not union_sql_list and keyword_list:
            sql = self.get_keyword_sql_by_name_list(keyword_list)
            sql += "".join(direct_add_sql_list)
            return " union " + sql
        last_sql = (
            " union " + " union ".join(union_sql_list) + "".join(direct_add_sql_list)
        )
        return last_sql

    def make_faq_sql(self, order_line_entity_info_list):
        k = 0
        sql = ""
        entity_sql_list = []
        located_sql_list = []
        union_sql_list = []
        for (
            order_line_entity_class,
            order_line_entity_list,
        ) in order_line_entity_info_list:
            if order_line_entity_list:
                for entity_class, entity in order_line_entity_list:
                    k += 1
                    sql = ' match (k%s:%s{name:"%s"}) ' % (str(k), entity_class, entity)
                    entity_sql_list.append(sql)
                    sql = (
                        ' match (k%s)-[rk%s:includedOf{name:"包含于"}]->(q1:FaqQuestion) '
                        % (str(k), str(k))
                    )
                    located_sql_list.append(sql)
                    if entity_class in self.entity_class_with_attr_list:
                        union_sql = (
                            ' match (k%s:%s{name:"%s"})<-[rk1:attributedOf]-(ak)   return distinct "8" as a,[ak,rk1,k%s] as b,null as c,null as d,'
                            " null as e,null as f, null as g "
                            % (str(k), entity_class, entity, str(k))
                        )
                        union_sql_list.append(union_sql)
                break
        if not sql and not union_sql_list:
            return sql
        sql = " ".join(entity_sql_list + located_sql_list)
        sql += (
            ' match (q1)<-[rq1:adaptedTo{name:"答案适配问题"}]-(a1:FaqAnswer)  return distinct "7" as a,[q1,rq1,a1] as b,null as c,null as d,'
            " null as e,null as f, null as g  "
        )
        if union_sql_list:
            sql += " union " + " union ".join(union_sql_list)
        return sql

    def cal_kbqa(
        self,
        kbqa_result,
        ori_user_code,
        org_list,
        dep_list,
        pos_list,
        key_list,
        input_odp_class_set,
        input_odp_set,
    ):
        result_dict = defaultdict()
        user_name = kbqa_result["user_name"]
        s_new_name_new_code_dict = kbqa_result["s_new_name_new_code_dict"]
        s_new_name_name_dict = kbqa_result["s_new_name_name_dict"]
        s_new_code_class_dict = kbqa_result["s_new_code_class_dict"]
        new_code_name_dict = kbqa_result["new_code_name_dict"]
        employee_some_info_dict = kbqa_result["employee_some_info_dict"]
        department_parent_info = kbqa_result["department_parent_info"]
        organization_parent_info = kbqa_result["organization_parent_info"]
        knowledge_fuzzy_name_dict = kbqa_result["knowledge_fuzzy_name_dict"]
        knowledge_dict = kbqa_result["knowledge_dict"]
        # 计算同名实体相关度得分，计算参考维度：岗位，岗位条线，部门和机构
        knowledge_score_dict = defaultdict(dict)  # 存放实体得分
        knowledge_info_dict = defaultdict(dict)  # 存放实体知识
        knowledge_card_info_dict = defaultdict(dict)  # 存放实体卡片属性知识
        knowledge_target_dict = defaultdict(list)  # 存放完全符合输入的目标实体

        department_code_org_code_dict = {}
        employee_rel_score_init = defaultdict(int)  # 用于将人员的相关度得分初始化其部门和机构
        user_position = employee_some_info_dict.get(ori_user_code, {}).get("工作岗位", "")
        user_line_position = employee_some_info_dict.get(ori_user_code, {}).get(
            "岗位条线名称", ""
        )
        user_position_list = [
            item for item in [user_position, user_line_position] if item
        ]
        user_org_code = employee_some_info_dict.get(ori_user_code, {}).get("所属机构", "")
        user_org = new_code_name_dict.get(user_org_code, "")
        user_parent_org_code = organization_parent_info.get(user_org_code, {}).get(
            "所属直属上级机构", ""
        )
        # user_parent_org = new_code_name_dict.get(user_parent_org_code, "")
        user_dept_code = employee_some_info_dict.get(ori_user_code, {}).get("所属部门", "")
        user_dept = new_code_name_dict.get(user_dept_code, "")
        # user_first_parent_dept = department_parent_info.get(user_dept_code, {}).get(
        #     "所属直属上级部门", ""
        # )
        user_first_parent_dept = employee_some_info_dict.get(ori_user_code, {}).get(
            "所属直属上级部门", ""
        )

        # user_second_parent_dept = department_parent_info.get(user_dept_code, {}).get(
        #     "所属二级上级部门", ""
        # )
        user_second_parent_dept = employee_some_info_dict.get(ori_user_code, {}).get(
            "所属二级上级部门", ""
        )
        user_parent_dept_list = [
            item
            for item in [
                user_dept_code,
                user_first_parent_dept,
                user_second_parent_dept,
            ]
            if item
        ]
        user_parent_org_list = [
            item for item in [user_org_code, user_parent_org_code] if item
        ]
        for s_new_name in knowledge_dict:
            s_new_code = s_new_name_new_code_dict.get(s_new_name)
            s_name = s_new_name_name_dict.get(s_new_name, "")
            s_class = s_new_code_class_dict.get(s_new_code, "Other")
            if s_new_code not in employee_some_info_dict:
                continue
            if s_new_code == ori_user_code:
                knowledge_score_dict[s_class][s_new_code] = [user_name, 1]
                continue
            s_position = employee_some_info_dict.get(s_new_code, {}).get("工作岗位", "")
            s_line_position = employee_some_info_dict.get(s_new_code, {}).get(
                "岗位条线名称", ""
            )
            s_position_list = [item for item in [s_position, s_line_position] if item]
            s_org_code = employee_some_info_dict.get(s_new_code, {}).get("所属机构", "")
            s_org = new_code_name_dict.get(s_org_code, "")

            s_parent_org_code = employee_some_info_dict.get(s_new_code, {}).get(
                "所属直属上级机构", ""
            )
            if not s_parent_org_code:
                s_parent_org_code = organization_parent_info.get(s_new_code, {}).get(
                    "所属直属上级机构", ""
                )

            s_parent_org = new_code_name_dict.get(s_parent_org_code, "")
            s_dept_code = employee_some_info_dict.get(s_new_code, {}).get("所属部门", "")
            if s_dept_code and s_org_code:
                department_code_org_code_dict[s_dept_code] = s_org_code
            s_dept = new_code_name_dict.get(s_dept_code, "")
            s_first_parent_dept = employee_some_info_dict.get(s_new_code, {}).get(
                "所属直属上级部门", ""
            )
            s_second_parent_dept = employee_some_info_dict.get(s_new_code, {}).get(
                "所属二级上级部门", ""
            )
            if not s_first_parent_dept and not s_second_parent_dept:
                s_first_parent_dept = department_parent_info.get(s_new_code, {}).get(
                    "所属直属上级部门", ""
                )
                s_second_parent_dept = department_parent_info.get(s_new_code, {}).get(
                    "所属二级上级部门", ""
                )
            s_parent_dept_list = [
                item
                for item in [s_dept_code, s_first_parent_dept, s_second_parent_dept]
                if item
            ]
            s_parent_org_list = [
                item for item in [s_org_code, s_parent_org_code] if item
            ]
            s_score = 0
            s_score_factor = 1
            if "Organization" in input_odp_class_set and org_list:
                if user_org not in input_odp_set:
                    if s_dept_code and s_dept_code in input_odp_set:
                        s_score = 0.8
                    elif s_dept and s_dept in input_odp_set:
                        s_score = 0.7
                    elif (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.2
                    if (
                        s_org_code
                        and s_org_code in input_odp_set
                        or s_org
                        and s_org in input_odp_set
                    ):
                        if s_score:
                            s_score = s_score + 0.1
                        else:
                            s_score = 0.6
                    elif s_parent_org and s_parent_org in input_odp_set:
                        if s_score:
                            s_score = s_score + 0.1
                        else:
                            s_score = 0.5
                    else:
                        s_org_cut_result = set(self.jieba.cut(s_org))
                        num = 0
                        total_org_str = " ".join(org_list)
                        for item in s_org_cut_result:
                            if item and total_org_str and item in total_org_str:
                                num += 1
                        s_score += 0.35 * num
                        s_score = min(s_score, 0.8)
                    if (s_position and s_position in pos_list) or (
                        s_line_position and s_line_position in input_odp_set
                    ):
                        s_score += 0.4
                        s_score = min(s_score, 0.9)
                    else:
                        for s_pos, inp_odp in product(
                            s_position_list, list(input_odp_set)
                        ):
                            if s_pos in inp_odp or inp_odp in s_pos:
                                s_score += 0.3
                                s_score = min(s_score, 0.9)
                                break
                    s_score = s_score * 0.9
                    s_score_factor = 0.1
                else:
                    if s_dept_code and s_dept_code in input_odp_set:
                        s_score = 0.7
                    elif s_dept and s_dept in input_odp_set:
                        s_score = 0.6
                    elif (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.3
                    if (
                        s_org_code
                        and s_org_code in input_odp_set
                        or s_org
                        and s_org in input_odp_set
                    ):
                        if s_score:
                            s_score = s_score + 0.1
                        else:
                            s_score = 0.4
                    elif s_parent_org and s_parent_org in input_odp_set:
                        if s_score:
                            s_score = s_score + 0.1
                        else:
                            s_score = 0.3
                    else:
                        s_org_cut_result = set(self.jieba.cut(s_org))
                        num = 0
                        total_org_str = " ".join(org_list)
                        for item in s_org_cut_result:
                            if item and total_org_str and item in total_org_str:
                                num += 1
                        s_score += 0.35 * num
                        s_score = min(s_score, 0.7)
                    if (s_position and s_position in input_odp_set) or (
                        s_line_position and s_line_position in input_odp_set
                    ):
                        s_score += 0.3
                        s_score = min(s_score, 0.9)
                    else:
                        for s_pos, inp_odp in product(
                            s_position_list, list(input_odp_set)
                        ):
                            if s_pos in inp_odp or inp_odp in s_pos:
                                s_score += 0.2
                                s_score = min(s_score, 0.9)
                                break
                    s_score = s_score * 0.7
                    s_score_factor = 0.3
            elif "Department" in input_odp_class_set and dep_list:
                if user_dept not in input_odp_set:
                    if s_dept_code and s_dept_code in input_odp_set:
                        s_score = 0.7
                    elif s_dept and s_dept in input_odp_set:
                        s_score = 0.6
                    elif s_parent_dept_list:
                        for item in s_parent_dept_list:
                            if new_code_name_dict.get(item, "") in input_odp_set:
                                s_score = 0.65
                    if not s_score and (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.3
                    if (s_position and s_position in input_odp_set) or (
                        s_line_position and s_line_position in input_odp_set
                    ):
                        s_score += 0.4
                        s_score = min(s_score, 0.9)
                    else:
                        for s_pos, inp_odp in product(
                            s_position_list, list(input_odp_set)
                        ):
                            if s_pos in inp_odp or inp_odp in s_pos:
                                s_score += 0.3
                                s_score = min(s_score, 0.9)
                                break
                    s_score = s_score * 0.9
                    s_score_factor = 0.1
                else:
                    if s_dept_code and s_dept_code in input_odp_set:
                        s_score = 0.7
                    elif s_dept and s_dept in input_odp_set:
                        s_score = 0.6
                    elif s_parent_dept_list:
                        for item in s_parent_dept_list:
                            if new_code_name_dict.get(item, "") in input_odp_set:
                                s_score = 0.65
                    if not s_score and (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.5
                    if (s_position and s_position in input_odp_set) or (
                        s_line_position and s_line_position in input_odp_set
                    ):
                        s_score += 0.3
                        s_score = min(s_score, 0.9)
                    else:
                        for s_pos, inp_odp in product(
                            s_position_list, list(input_odp_set)
                        ):
                            if s_pos in inp_odp or inp_odp in s_pos:
                                s_score += 0.2
                                s_score = min(s_score, 0.9)
                                break
                    s_score = s_score * 0.7
                    s_score_factor = 0.3
            elif "Position" in input_odp_class_set and pos_list:
                get_flag = 0
                if (s_position and s_position in input_odp_set) or (
                    s_line_position and s_line_position in input_odp_set
                ):
                    s_score = 1
                    get_flag = 1
                else:
                    for s_pos, inp_odp in product(s_position_list, list(input_odp_set)):
                        if s_pos in inp_odp or inp_odp in s_pos:
                            s_score = 0.7
                            get_flag = 1
                            break
                if get_flag:
                    s_score = s_score * 0.7
                    s_score_factor = 0.3
                else:
                    s_score_factor = 0.5
            if key_list:
                # 默认为人员职责，按机构、部门排序
                mid_score = 0
                user_duty = employee_some_info_dict.get(s_new_code, {}).get("工作职责", "")
                num = [1 for keyword in key_list if keyword in user_duty].count(1)
                mid_score += 0.2 * num
                if len(list(set(user_parent_dept_list) & set(s_parent_dept_list))) > 0:
                    mid_score += 0.5
                elif user_org_code == s_org_code:
                    mid_score += 0.3
                # 关键词是否在机构或部门名称里
                num = [
                    1 for keyword in key_list if keyword in s_org or keyword in s_dept
                ].count(1)
                mid_score += 0.25 * num
                s_score = s_score + mid_score * 0.7
                s_score = min(s_score, 0.9)
                if s_score_factor == 1:
                    s_score = s_score * 0.8
                    s_score_factor = 0.2
            if s_org_code and user_org_code and s_org_code == user_org_code:
                s_score = s_score + 0.25 * s_score_factor
            if s_dept_code and user_dept_code and s_dept_code == user_dept_code:
                s_score = s_score + 0.6 * s_score_factor
            elif len(list(set(user_parent_dept_list) & set(s_parent_dept_list))) > 0:
                s_score = s_score + 0.3 * s_score_factor
            if len(list(set(user_position_list) & set(s_position_list))) > 0:
                s_score = s_score + 0.1 * s_score_factor
            # 在什么条件都不具备的情况下，总部的人员占略微优势
            if s_score <= 0.5 and s_org_code == "999999":
                s_score += 0.01
            knowledge_score_dict[s_class][s_new_code] = [s_name, s_score]
            if s_dept_code:
                employee_rel_score_init[s_dept_code] = s_score
            if s_org_code:
                employee_rel_score_init[s_org_code] = s_score
            s_total_info_list = s_position_list + [s_dept, s_org]
            input_total_info_list = org_list + dep_list + pos_list
            if set(input_total_info_list).issubset(set(s_total_info_list)):
                knowledge_target_dict[s_class].append([s_name, s_new_code])

        for s_new_name in knowledge_dict:
            s_new_code = s_new_name_new_code_dict.get(s_new_name)
            s_name = s_new_name_name_dict.get(s_new_name, "")
            s_class = s_new_code_class_dict.get(s_new_code, "Other")
            if s_new_code in employee_some_info_dict:
                continue
            s_score = 0
            if s_class == "Department":
                s_first_parent_dept = department_parent_info.get(s_new_code, {}).get(
                    "所属直属上级部门", ""
                )
                s_second_parent_dept = department_parent_info.get(s_new_code, {}).get(
                    "所属二级上级部门", ""
                )
                s_org_code = department_code_org_code_dict.get(s_new_code, "")
                s_org = new_code_name_dict.get(s_org_code, "")
                s_parent_org_code = organization_parent_info.get(s_org_code, {}).get(
                    "所属直属上级机构", ""
                )
                s_parent_org = new_code_name_dict.get(s_parent_org_code, "")
                s_parent_dept_list = [
                    item
                    for item in [s_new_code, s_first_parent_dept, s_second_parent_dept]
                    if item
                ]
                if user_dept_code not in input_odp_set:
                    if s_new_code and s_new_code in input_odp_set:
                        s_score = 0.7
                    elif s_name and s_name in input_odp_set:
                        s_score = 0.5
                    elif (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.2
                    if s_org and s_org in input_odp_set:
                        s_score += 0.3
                    elif s_parent_org and s_parent_org in input_odp_set:
                        s_score += 0.3
                    else:
                        s_org_cut_result = set(self.jieba.cut(s_org))
                        num = 0
                        total_org_str = " ".join(org_list)
                        for item in s_org_cut_result:
                            if item and total_org_str and item in total_org_str:
                                num += 1
                        s_score += 0.2 * num
                        s_score = min(s_score, 0.8)
                else:
                    if s_new_code and s_new_code in input_odp_set:
                        s_score = 0.6
                    elif s_name and s_name in input_odp_set:
                        s_score = 0.5
                    elif (
                        len(list(set(user_parent_dept_list) & set(s_parent_dept_list)))
                        > 0
                    ):
                        s_score = 0.3
                    if s_org and s_org in input_odp_set:
                        s_score += 0.3
                    elif s_parent_org and s_parent_org in input_odp_set:
                        s_score += 0.3
                    else:
                        s_org_cut_result = set(self.jieba.cut(s_org))
                        num = 0
                        total_org_str = " ".join(org_list)
                        for item in s_org_cut_result:
                            if item and total_org_str and item in total_org_str:
                                num += 1
                        s_score += 0.2 * num
                        s_score = min(s_score, 0.8)
                score_init = employee_rel_score_init.get(s_new_code, 0)
                if score_init:
                    s_score = s_score * 0.2 + score_init * 0.8
                    s_score = min(s_score, 0.9)
                knowledge_score_dict[s_class][s_new_code] = [s_name, s_score]
                if (not org_list or s_org in org_list) and (
                    not dep_list or s_name in dep_list
                ):
                    knowledge_target_dict[s_class].append([s_name, s_new_code])
            elif s_class == "Organization":
                s_parent_org_code = organization_parent_info.get(s_new_code, {}).get(
                    "所属直属上级机构", ""
                )
                s_parent_org = new_code_name_dict.get(s_parent_org_code, "")
                if s_name in input_odp_set:
                    s_score = 1
                elif s_parent_org in input_odp_set:
                    s_score = 0.5
                elif s_new_code and user_org_code and s_new_code == user_org_code:
                    s_score = 0.4
                else:
                    s_org_cut_result = set(self.jieba.cut(s_name))
                    num = 0
                    total_org_str = " ".join(org_list)
                    for item in s_org_cut_result:
                        if item and item in total_org_str:
                            num += 1
                    s_score += 0.35 * num
                    s_score = min(s_score, 1)
                score_init = employee_rel_score_init.get(s_new_code, 0)
                if score_init:
                    s_score = s_score * 0.2 + score_init * 0.8
                    s_score = min(s_score, 0.9)
                knowledge_score_dict[s_class][s_new_code] = [s_name, s_score]

        for s_name in knowledge_dict:
            s_new_code = s_new_name_new_code_dict.get(s_name)
            s_class = s_new_code_class_dict.get(s_new_code, "Other")
            s_info_dict = defaultdict(list)
            for p_class, o_key_value_set in knowledge_dict[s_name].items():
                for key, value in o_key_value_set:
                    if s_class == "Employee":
                        p_name = "姓名"
                    else:
                        p_name = "名称"
                    p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                    if key == p_name:
                        s_info_dict["name"] = value
                    elif key == "所属机构代码":
                        s_info_dict["org_code"].append(value)
                    elif key == "所属部门代码":
                        s_info_dict["dept_code"].append(value)
                    if s_class == "Employee":
                        if key in self.employee_public_attribute_list:
                            s_info_dict["public"].append([key, value])
                        elif self.permission:
                            s_info_dict["public"].append([key, value])
                        else:
                            s_info_dict["permission"].append([key, value])
                        if key in self.employee_card_attribute_list:
                            knowledge_card_info_dict[s_new_code][key] = value
                    elif s_class == "Department":
                        if key in self.department_permission_attribute_list:
                            s_info_dict["permission"].append([key, value])
                        else:
                            s_info_dict["public"].append([key, value])
                    elif s_class == "Organization":
                        if key in self.organization_permission_attribute_list:
                            s_info_dict["permission"].append([key, value])
                        else:
                            s_info_dict["public"].append([key, value])
                    else:
                        s_info_dict["public"].append([key, value])
            knowledge_info_dict[s_class][s_new_code] = s_info_dict

        result_dict["knowledge_score_dict"] = knowledge_score_dict
        result_dict["knowledge_info_dict"] = knowledge_info_dict
        result_dict["knowledge_target_dict"] = knowledge_target_dict
        result_dict["knowledge_card_info_dict"] = knowledge_card_info_dict
        result_dict["knowledge_fuzzy_name_dict"] = knowledge_fuzzy_name_dict
        result_dict["user_name"] = user_name
        return result_dict

    def cal_faq(self, faq_result, ori_user_code, order_line_entity_info_list):
        knowledge_score_dict = defaultdict(dict)  # 存放实体得分
        knowledge_info_dict = defaultdict(dict)  # 存放实体知识
        knowledge_card_info_dict = defaultdict(dict)  # 存放实体卡片属性知识
        knowledge_target_dict = defaultdict(list)  # 存放完全符合输入的目标实体
        user_name = faq_result["user_name"]
        knowledge_dict = faq_result["knowledge_dict"]
        question_answer_dict = faq_result["question_answer_dict"]
        employee_some_info_dict = faq_result["employee_some_info_dict"]
        new_code_name_dict = faq_result["new_code_name_dict"]
        s_new_name_new_code_dict = faq_result["s_new_name_new_code_dict"]
        s_new_code_class_dict = faq_result["s_new_code_class_dict"]
        knowledge_fuzzy_name_dict = faq_result["knowledge_fuzzy_name_dict"]
        user_attr_info_list = []
        user_info_dict = employee_some_info_dict.get(ori_user_code, {})

        for p_name in user_info_dict:
            if p_name in ["工作岗位", "岗位条线名称"]:
                user_attr_info_list.append(user_info_dict[p_name])
            elif p_name in ["所属部门", "所属机构"]:
                s_code = user_info_dict[p_name]
                s_name = new_code_name_dict.get(s_code, "")
                if s_name:
                    user_attr_info_list.append(s_name)

        for s_name in knowledge_dict:
            s_new_code = s_new_name_new_code_dict.get(s_name)
            s_class = s_new_code_class_dict.get(s_new_code, "Other")
            s_info_dict = defaultdict(list)
            for p_class, o_key_value_set in knowledge_dict[s_name].items():
                for key, value in o_key_value_set:
                    if s_class == "Employee":
                        p_name = "姓名"
                    else:
                        p_name = "名称"
                    p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                    if key == p_name:
                        s_info_dict["name"] = value
                    elif key == "所属机构代码":
                        s_info_dict["org_code"].append(value)
                    elif key == "所属部门代码":
                        s_info_dict["dept_code"].append(value)
                    if s_class == "Employee":
                        if key in self.employee_public_attribute_list:
                            s_info_dict["public"].append([key, value])
                        else:
                            s_info_dict["permission"].append([key, value])
                        if key in self.employee_card_attribute_list:
                            knowledge_card_info_dict[s_new_code][key] = value
                    elif s_class == "Department":
                        if key in self.department_permission_attribute_list:
                            s_info_dict["permission"].append([key, value])
                        else:
                            s_info_dict["public"].append([key, value])
                    elif s_class == "Organization":
                        if key in self.organization_permission_attribute_list:
                            s_info_dict["permission"].append([key, value])
                        else:
                            s_info_dict["public"].append([key, value])
                    else:
                        s_info_dict["public"].append([key, value])
            knowledge_info_dict[s_class][s_new_code] = s_info_dict

        max_score = 0
        max_score_qa_set = set()
        qa_list = []
        for question in question_answer_dict:
            score = 0
            for user_attr_info in user_attr_info_list:
                if user_attr_info in question:
                    score += 1
            for order, order_line_entity_info in enumerate(order_line_entity_info_list):
                order += 1
                order_line_entity_class, order_line_entity_list = order_line_entity_info
                for _, order_line_entity in order_line_entity_list:
                    if order_line_entity in question:
                        score += self.order_score_dict[order]
                        # 可以加入减分机制
                        if score > max_score:
                            max_score = score
                            max_score_qa_set = set()
                            max_score_qa_set.add(
                                (question, question_answer_dict.get(question))
                            )
                            qa_list = []
                            qa_list.append(
                                [question, question_answer_dict.get(question)]
                            )
                        elif score == max_score:
                            max_score_qa_set.add(
                                (question, question_answer_dict.get(question))
                            )
                            qa_list.append(
                                [question, question_answer_dict.get(question)]
                            )
            knowledge_score_dict["FaqQuestion"][question] = [question, score]
            knowledge_info_dict["FaqQuestion"][question] = question_answer_dict.get(
                question
            )
        result_dict = {}
        result_dict["knowledge_score_dict"] = knowledge_score_dict
        result_dict["knowledge_info_dict"] = knowledge_info_dict
        result_dict["knowledge_target_dict"] = knowledge_target_dict
        result_dict["knowledge_card_info_dict"] = knowledge_card_info_dict
        result_dict["knowledge_fuzzy_name_dict"] = knowledge_fuzzy_name_dict
        result_dict["user_name"] = user_name
        return result_dict

    def cal_kbqa_value(self, kbqa_result, intention, target_entity):
        result_dict = defaultdict()
        user_name = kbqa_result["user_name"]
        s_new_name_new_code_dict = kbqa_result["s_new_name_new_code_dict"]
        s_new_name_name_dict = kbqa_result["s_new_name_name_dict"]
        s_new_code_class_dict = kbqa_result["s_new_code_class_dict"]
        s_new_code_pro_dict = kbqa_result["s_new_code_pro_dict"]
        knowledge_fuzzy_name_dict = kbqa_result["knowledge_fuzzy_name_dict"]
        knowledge_dict = kbqa_result["knowledge_dict"]
        # 计算同名实体相关度得分，计算参考维度：岗位，岗位条线，部门和机构
        knowledge_score_dict = defaultdict(dict)  # 存放实体得分
        knowledge_info_dict = defaultdict(dict)  # 存放实体知识
        knowledge_card_info_dict = defaultdict(dict)  # 存放实体卡片属性知识
        knowledge_target_dict = defaultdict(list)  # 存放完全符合输入的目标实体

        for s_new_name in knowledge_dict:
            s_new_code = s_new_name_new_code_dict.get(s_new_name, "")
            s_name = s_new_name_name_dict.get(s_new_name, s_new_name)
            s_class = s_new_code_class_dict.get(s_new_code, "Other")
            s_info_dict = defaultdict(list)
            knowledge_score_dict[s_class][s_new_code] = [s_name, 1]
            for p_class, o_key_value_set in knowledge_dict[s_new_name].items():
                for key, value in o_key_value_set:
                    if s_class == "Employee":
                        p_name = "姓名"
                    else:
                        p_name = "名称"
                    p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                    if key == p_name:
                        s_info_dict["name"] = value
                    s_info_dict["public"].append([key, value])
            knowledge_info_dict[s_class][s_new_code] = s_info_dict

        new_knowledge_info_dict = defaultdict(dict)
        # 查值场景
        if (
            intention == "value"
            and target_entity == "Indicator"
            and knowledge_info_dict
        ):
            legal_attr_name_dict = {
                "Employee": ["员工姓名", "工作岗位", "所属机构", "所属部门"],
                "Organization": ["机构名称", "所属直属上级机构"],
                "Indicator": ["衍生于", "分子", "分母", "指标名称"],
            }
            for s_class in knowledge_info_dict:
                for s_new_code in knowledge_info_dict[s_class]:
                    s_info_dict = defaultdict(list)
                    for item in knowledge_info_dict[s_class][s_new_code]["public"]:
                        key, value = item[0], item[1]
                        if s_class == "Employee":
                            p_name = "姓名"
                        else:
                            p_name = "名称"
                        p_name = self.entity_class_name_dict.get(s_class, "") + p_name
                        if key == p_name:
                            s_info_dict["name"] = value
                        if key in legal_attr_name_dict.get(s_class, []):
                            s_info_dict["public"].append([key, value])
                    # 加入指标属性
                    if s_class == "Indicator":
                        s_pro = s_new_code_pro_dict.get(s_new_code, {})
                        for key, value in s_pro.items():
                            if key in self.indicator_value_inner_name_dict and value:
                                s_info_dict["public"].append(
                                    [self.indicator_value_inner_name_dict[key], value]
                                )
                    new_knowledge_info_dict[s_class][s_new_code] = s_info_dict

        knowledge_info_dict = new_knowledge_info_dict
        result_dict["knowledge_score_dict"] = knowledge_score_dict
        result_dict["knowledge_info_dict"] = knowledge_info_dict
        result_dict["knowledge_target_dict"] = knowledge_target_dict
        result_dict["knowledge_card_info_dict"] = knowledge_card_info_dict
        result_dict["knowledge_fuzzy_name_dict"] = knowledge_fuzzy_name_dict
        result_dict["user_name"] = user_name
        return result_dict

    def get_fuzzy_entity(self, entity, entity_class, entity_tree, entity_dict):
        fuzzy_entity_list = []
        entity_tree.startsWith(entity)
        fuzzy_entity_list_one = entity_tree.satis_word_list
        fuzzy_entity_list_one = list(set(fuzzy_entity_list_one))
        fuzzy_entity_list_one.sort(key=len)
        fuzzy_entity_list += fuzzy_entity_list_one[:2]
        if entity_class == "Position" and "Department" in entity_dict:
            for item in entity_dict["Department"]:
                if not item:
                    continue
                item_value, _ = item
                if not item_value:
                    continue
                entity_tree.startsWith(item_value + entity)
                fuzzy_entity_list_two = entity_tree.satis_word_list
                fuzzy_entity_list_two = list(set(fuzzy_entity_list_two))
                fuzzy_entity_list_two.sort(key=len)
                fuzzy_entity_list = (
                    fuzzy_entity_list
                    + fuzzy_entity_list_two[:1]
                    + [item_value + entity]
                )
        entity_tree.startsWith(entity[::-1])
        fuzzy_entity_list_three = entity_tree.satis_word_list
        fuzzy_entity_list_three = [item[::-1] for item in fuzzy_entity_list_three]
        fuzzy_entity_list_three = list(set(fuzzy_entity_list_three))
        fuzzy_entity_list_three.sort(key=len)
        fuzzy_entity_list += fuzzy_entity_list_three[:2]
        fuzzy_entity_list = list(set(fuzzy_entity_list))
        return fuzzy_entity_list

    def run(self, info_dict: dict) -> dict:
        """
        主函数
        :param info_dict:输入信息dict
        :return:
        """
        sql_entity_name_class_dict = {}
        input_odp_set = set()
        input_odp_class_set = set()
        hop_pair_list = []
        emp_list = []
        org_list = []
        dep_list = []
        pos_list = []
        key_list = []
        cod_list = []
        result_dict = defaultdict()
        # 知识类必须按照如下顺序排序，否则很影响速度
        order_entity_class_list = [
            "Organization",
            "Department",
            "Employee",
            "Code",
            "Keyword",
        ]
        undone_entity_class_attr_dict = defaultdict(dict)
        kbqa_flag = 1
        sql = ""
        input_fuzzy_entity_class_list = []

        ori_user_code = info_dict.get("user_code", "")
        question = info_dict.get("question", "")
        intention = info_dict.get("intention", "info")
        if not intention:
            intention = "info"
        target_entity = info_dict.get("target_entity", "Employee")
        if not target_entity:
            intention = "Employee"
        self.ori_user_code = ori_user_code
        self.user_code = ori_user_code
        try:
            self.user_code = str(int(self.user_code))
        except:
            pass
        knowledge_dict = defaultdict(dict)
        entity_dict = info_dict.get("entity_info", {})
        relationship_list = info_dict.get("relationship_info", [])
        entity_dict_copy = defaultdict(list)

        # 指标查询
        if (
            target_entity == "Indicator"
            and intention == "value"
            and "Indicator" in entity_dict
        ):
            org_list = [item[0] for item in entity_dict.get("Organization", []) if item]
            indicator_list = [
                item[0] for item in entity_dict.get("Indicator", []) if item
            ]
            if indicator_list:
                entity_class_info_set_dict = {
                    "Organization": (org_list, [], {}),
                    "Indicator": (indicator_list, [], {}),
                }
                sql = self.make_kbqa_sql(
                    order_entity_class_list,
                    entity_class_info_set_dict,
                    {},
                    input_fuzzy_entity_class_list,
                    hop_pair_list,
                    intention,
                    target_entity,
                )
                sql = self.add_user_sql(sql)
                _ = self.info_sql_run_and_parse(
                    sql, knowledge_dict, sql_entity_name_class_dict
                )
                result_dict = self.cal_kbqa_value(_, intention, target_entity)
                result_dict["kbqa_flag"] = kbqa_flag
                return result_dict

        # 规整输入参数
        if "Keyword" in entity_dict:
            del entity_dict["Keyword"]
        all_entity_value_list = []
        drop_entity_class_input_dict = defaultdict(list)
        rk_list = []
        for entity_class, _ in entity_dict.items():
            if not _:
                continue
            total_num = sum([1 for item in _ if item])
            if not total_num:
                continue
            if entity_class == "RelationshipKeyword":
                rk_list = [item[0] for item in _ if item[0] != "工作"]
                continue
            for item in _:
                if not len(item) == 2:
                    continue
                fuzzy_entity_list = []
                entity, source = item[0], item[1]
                if not entity:
                    continue
                entity_tree = self.tree_dict.get(entity_class, None)
                if source == 1 and entity_tree:
                    fuzzy_entity_list = self.get_fuzzy_entity(
                        entity, entity_class, entity_tree, entity_dict
                    )
                    input_fuzzy_entity_class_list.append(entity_class)
                if entity_class == "Keyword":
                    # 找人的场景限定
                    if intention == "find" and target_entity == "Employee":
                        entity_dict_copy[entity_class].append(entity)
                        continue
                    continue
                if entity in self.need_check_entity_list:
                    if entity == "计算机" and "Skill" in entity_dict:
                        continue
                    elif entity == "没有" and "有没有" in question:
                        continue
                    elif entity == "否" and "是否" in question:
                        continue
                entity_dict_copy[entity_class].append(entity)
                if fuzzy_entity_list:
                    entity_dict_copy[entity_class].extend(fuzzy_entity_list)
                all_entity_value_list.append(entity)
        for entity_class, _ in entity_dict.items():
            if not _:
                continue
            total_num = sum([1 for item in _ if item])
            if not total_num:
                continue
            for item in _:
                if not len(item) == 2:
                    continue
                entity, source = item[0], item[1]
                if not entity:
                    continue
                for bean in all_entity_value_list:
                    if (
                        entity != bean
                        and entity in bean
                        and entity_class
                        not in ["Organization", "Department", "Position"]
                    ):
                        drop_entity_class_input_dict[entity_class].append(entity)

        for entity_class, entity_info_list in drop_entity_class_input_dict.items():
            for item in entity_info_list:
                if item in entity_dict_copy[entity_class]:
                    entity_dict_copy[entity_class].remove(item)
        entity_dict = entity_dict_copy

        if rk_list and target_entity == "Employee":
            if not self.permission_flag:
                self.get_employee_permission()
            for item in rk_list:
                self.employee_related_permission_attribute_list.extend(
                    self.employee_permission_key_attribute_dict.get(item, [])
                )

        # 补充实体
        for word, word_assert_attr in self.employee_assert_key_attribute_dict.items():
            if word in question and target_entity == "Employee":
                # 权限校验
                if not self.permission_flag:
                    self.get_employee_permission()
                if "Negation" not in entity_dict:
                    self.employee_assert_attr_value_dict[word_assert_attr] = "是"
                else:
                    self.employee_assert_attr_value_dict[word_assert_attr] = "否"
                if self.permission:
                    self.employee_related_permission_attribute_list.append(
                        word_assert_attr
                    )
                entity_dict["Assert"].append(word_assert_attr)

        if "Assert" in entity_dict:
            entity_dict["Assert"] = list(set(entity_dict["Assert"]))

        age_number_flag = False
        for compare_attr_class in self.employee_compare_attr_class_list:
            if compare_attr_class in entity_dict:
                compare_list = entity_dict.get("Compare", [])
                first_compare_index = 0
                compare = ""
                if len(compare_list) > 1:
                    compare = "之间"
                    first_compare_index = question.index(compare_list[0])
                elif len(compare_list) == 1:
                    compare = compare_list[0]
                    first_compare_index = question.index(compare_list[0])
                compare = self.compare_word_logic_dict.get(compare, "")
                item_list = entity_dict[compare_attr_class]
                if compare_attr_class in ["Age", "Number"]:
                    rel_name = ""
                    if compare_attr_class == "Age":
                        rel_name = "年龄"
                    elif "身高" in rk_list:
                        rel_name = "身高"
                        for key in self.employee_compare_key_attr_dict:
                            if key in question:
                                rel_name = self.employee_compare_key_attr_dict[key]
                                age_number_flag = True
                                break
                    elif "体重" in rk_list:
                        rel_name = "体重"
                        for key in self.employee_compare_key_attr_dict:
                            if key in question:
                                rel_name = self.employee_compare_key_attr_dict[key]
                                age_number_flag = True
                                break
                    elif (
                        compare_attr_class == "Number"
                        and "工作" in question
                        or "司龄" in question
                    ):
                        rel_name = "司龄"
                        for key in self.employee_compare_key_attr_dict:
                            if key in question:
                                rel_name = self.employee_compare_key_attr_dict[key]
                                break
                    if not rel_name:
                        continue
                    if self.permission:
                        self.employee_related_permission_attribute_list.append(rel_name)
                    if len(item_list) == 2 and compare == "|":
                        min_item = min(float(item_list[0]), float(item_list[1]))
                        max_item = max(float(item_list[0]), float(item_list[1]))
                        self.employee_compare_attr_range_dict[compare_attr_class] = (
                            rel_name,
                            min_item,
                            max_item,
                        )
                    elif len(item_list) == 1 and compare:
                        # 只取一个
                        self.employee_compare_attr_logic_dict[compare_attr_class] = (
                            rel_name,
                            item_list[0],
                            compare,
                        )
                    else:
                        self.employee_compare_attr_value_dict[compare_attr_class] = (
                            rel_name,
                            item_list,
                        )
                else:
                    compare_attr_info = self.employee_order_attr_dict.get(
                        compare_attr_class
                    )
                    if not compare_attr_info:
                        continue
                    rel_name, bean_list = compare_attr_info
                    if self.permission:
                        self.employee_related_permission_attribute_list.append(rel_name)
                        self.employee_related_permission_attribute_list.extend(
                            self.employee_permission_attribute_class_name_dict[
                                compare_attr_class
                            ]
                        )
                    if (
                        compare
                        and "以" in question
                        and first_compare_index > question.index(item_list[0])
                    ):
                        # 只取一个
                        legal_bean_list = self.get_range_by_logic(
                            bean_list, item_list[0], compare
                        )
                        if legal_bean_list:
                            self.employee_compare_attr_value_dict[
                                compare_attr_class
                            ] = (rel_name, legal_bean_list)
                    else:
                        self.employee_compare_attr_value_dict[compare_attr_class] = (
                            rel_name,
                            item_list,
                        )

        if age_number_flag and target_entity == "Employee" and not self.permission_flag:
            self.get_employee_permission()

        # 权限校验
        if target_entity == "Employee":
            for word in self.employee_permission_key_attribute_dict:
                if word in question:
                    self.related_flag = True
                    if not self.permission_flag:
                        self.get_employee_permission()
                    if self.permission:
                        self.employee_related_permission_attribute_list.extend(
                            self.employee_permission_key_attribute_dict[word]
                        )

        first_line_entity_list = []
        second_line_entity_list = []
        third_line_entity_list = []
        fourth_line_entity_list = []
        order_line_entity_info_list = [
            (self.first_line_entity_class, first_line_entity_list),
            (self.second_line_entity_class, second_line_entity_list),
            (self.third_line_entity_class, third_line_entity_list),
            (self.fourth_line_entity_class, fourth_line_entity_list),
        ]
        for entity_class, entity_list in entity_dict.items():
            if entity_class == "Country":
                self.entity_need_same_set = self.entity_need_same_set.union(
                    set(entity_list)
                )

            if (
                entity_class in self.employee_permission_attribute_class_name_list
                and target_entity == "Employee"
            ):
                if intention != "info" or emp_list:
                    self.related_flag = True
                    if not self.permission_flag:
                        self.get_employee_permission()
                    self.employee_related_permission_attribute_list.extend(
                        self.employee_permission_attribute_class_name_dict[entity_class]
                    )

            for entity in entity_list:
                for (
                    order_line_entity_class,
                    order_line_entity_list,
                ) in order_line_entity_info_list:
                    if entity_class in order_line_entity_class:
                        order_line_entity_list.append((entity_class, entity))
        faq_judge_entity_list = first_line_entity_list + second_line_entity_list
        faq_total_entity_list = (
            first_line_entity_list
            + second_line_entity_list
            + third_line_entity_list
            + fourth_line_entity_list
        )

        delete_entity_set = set()
        emp_attr_list = []
        org_attr_list = []
        dep_attr_list = []
        key_attr_list = []
        cod_attr_list = []

        emp_attr_info_dict = defaultdict(list)
        org_attr_info_dict = defaultdict(list)
        dep_attr_info_dict = defaultdict(list)
        key_attr_info_dict = defaultdict(list)
        cod_attr_info_dict = defaultdict(list)

        # 分配实体及实体关系，用于编写sql
        if "Employee" in entity_dict:
            emp_list = entity_dict["Employee"]
            emp_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                emp_list, "Employee", sql_entity_name_class_dict
            )
            for entity_class in entity_dict:
                if entity_class == "Employee":
                    continue
                if entity_class in [
                    "Code",
                    "Number",
                    "Address",
                    "Statement",
                    "Assert",
                    "Phone",
                    "TimeDate",
                ]:
                    continue
                if entity_class in self.entity_class_attr_class_dict["Employee"]:
                    ent_list = entity_dict[entity_class]
                    if entity_class == "Department":
                        dep_list = ent_list
                    elif entity_class == "Organization":
                        org_list = ent_list
                    elif entity_class == "Position":
                        pos_list = ent_list
                    ent_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                        ent_list, entity_class, sql_entity_name_class_dict
                    )
                    for emp in emp_list:
                        emp_attr_info_dict[emp].extend(ent_name_type_list)
                    input_odp_set = input_odp_set.union(set(ent_list))
                    input_odp_class_set.add(entity_class)
                    delete_entity_set.add(entity_class)
                    emp_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Organization"]:
                    org_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Department"]:
                    dep_attr_list.append(entity_class)
        if "Organization" in entity_dict:
            org_list = entity_dict["Organization"]
            org_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                org_list, "Organization", sql_entity_name_class_dict
            )
            for entity_class in entity_dict:
                if entity_class == "Organization":
                    continue
                if entity_class in self.entity_class_attr_class_dict["Organization"]:
                    ent_list = entity_dict[entity_class]
                    ent_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                        ent_list, entity_class, sql_entity_name_class_dict
                    )
                    for org in org_list:
                        org_attr_info_dict[org].extend(ent_name_type_list)
                    input_odp_set = input_odp_set.union(set(ent_list))
                    input_odp_class_set.add(entity_class)
                    delete_entity_set.add(entity_class)
                    org_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Employee"]:
                    emp_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Department"]:
                    dep_attr_list.append(entity_class)
        if "Department" in entity_dict:
            dep_list = entity_dict["Department"]
            dep_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                dep_list, "Department", sql_entity_name_class_dict
            )
            for entity_class in entity_dict:
                if entity_class == "Department":
                    continue
                ent_list = entity_dict[entity_class]
                if entity_class in self.entity_class_attr_class_dict["Department"]:
                    ent_name_type_list, sql_entity_name_class_dict = self.get_pair_list(
                        ent_list, entity_class, sql_entity_name_class_dict
                    )
                    for dep in dep_list:
                        dep_attr_info_dict[dep].extend(ent_name_type_list)
                    input_odp_set = input_odp_set.union(set(ent_list))
                    input_odp_class_set.add(entity_class)
                    delete_entity_set.add(entity_class)
                    dep_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Employee"]:
                    emp_attr_list.append(entity_class)
                elif entity_class in self.entity_class_attr_class_dict["Organization"]:
                    org_attr_list.append(entity_class)
        if "Keyword" in entity_dict:
            key_list = entity_dict["Keyword"]
        if "Code" in entity_dict:
            mid_list = entity_dict["Code"]
            for item in mid_list:
                try:
                    cod_list.append(str(int(item)))
                except:
                    cod_list.append(str(item))
        for item in delete_entity_set:
            del entity_dict_copy[item]
        entity_dict = entity_dict_copy.copy()
        entity_class_info_set_dict = {
            "Organization": (org_list, org_attr_list, org_attr_info_dict),
            "Department": (dep_list, dep_attr_list, dep_attr_info_dict),
            "Employee": (emp_list, emp_attr_list, emp_attr_info_dict),
            "Keyword": (key_list, key_attr_list, key_attr_info_dict),
            "Code": (cod_list, cod_attr_list, cod_attr_info_dict),
        }

        # 没有权限则直接返回
        if self.permission_flag and not self.permission:
            result_dict["permission"] = self.permission
            result_dict["permission_flag"] = self.permission_flag
            result_dict["kbqa_flag"] = False
            return result_dict

        if "曾" in question or "以前" in question and intention == "find":
            self.related_flag = True

        for entity_class in entity_dict:
            if entity_class in order_entity_class_list:
                continue
            for ent in self.entity_class_attr_class_dict:
                if entity_class in self.entity_class_attr_class_dict[ent]:
                    undone_entity_class_attr_dict[ent][entity_class] = entity_dict[
                        entity_class
                    ]

        kbqa_main_list = emp_list + org_list + dep_list + cod_list

        if target_entity == "Others" and faq_judge_entity_list:
            sql = self.make_faq_sql(order_line_entity_info_list)
            kbqa_flag = 0
        elif intention == "count" and kbqa_main_list and target_entity != "Others":
            sql = self.make_kbqa_sql(
                order_entity_class_list,
                entity_class_info_set_dict,
                undone_entity_class_attr_dict,
                input_fuzzy_entity_class_list,
                hop_pair_list,
                intention,
                target_entity,
            )
        elif "老板" in question and intention == "find" and target_entity == "Employee":
            sql = self.make_faq_sql(order_line_entity_info_list)
            kbqa_flag = 2
        elif emp_list and not faq_judge_entity_list and target_entity != "Others":
            sql = self.make_kbqa_sql(
                order_entity_class_list,
                entity_class_info_set_dict,
                undone_entity_class_attr_dict,
                input_fuzzy_entity_class_list,
                hop_pair_list,
                intention,
                target_entity,
            )
        elif intention == "find" and target_entity == "Employee":
            sql = self.make_kbqa_sql(
                order_entity_class_list,
                entity_class_info_set_dict,
                undone_entity_class_attr_dict,
                input_fuzzy_entity_class_list,
                hop_pair_list,
                intention,
                target_entity,
            )
            kbqa_flag = 2
        elif faq_judge_entity_list and not kbqa_main_list:
            sql = self.make_faq_sql(order_line_entity_info_list)
            kbqa_flag = 0
        elif kbqa_main_list and intention == "find":
            sql = self.make_kbqa_sql(
                order_entity_class_list,
                entity_class_info_set_dict,
                undone_entity_class_attr_dict,
                input_fuzzy_entity_class_list,
                hop_pair_list,
                intention,
                target_entity,
            )
            kbqa_flag = 2
        elif faq_judge_entity_list and not kbqa_main_list:
            sql = self.make_faq_sql(order_line_entity_info_list)
            kbqa_flag = 0
        elif kbqa_main_list:
            sql = self.make_kbqa_sql(
                order_entity_class_list,
                entity_class_info_set_dict,
                undone_entity_class_attr_dict,
                input_fuzzy_entity_class_list,
                hop_pair_list,
                intention,
                target_entity,
            )
            kbqa_flag = 2
        else:
            if faq_total_entity_list:
                sql = self.make_faq_sql(order_line_entity_info_list)
            kbqa_flag = 0
        sql = self.add_user_sql(sql)

        _ = self.info_sql_run_and_parse(sql, knowledge_dict, sql_entity_name_class_dict)
        find_employee_set = _.get("find_employee_set", set())
        diff_set = set(emp_list).difference(find_employee_set)
        if emp_list:
            if (
                diff_set
                and target_entity == "Employee"
                and intention == "info"
                and kbqa_flag
            ):
                # 存在人找不到的情况，走拼音模糊匹配
                sql = self.make_kbqa_sql(
                    order_entity_class_list,
                    entity_class_info_set_dict,
                    undone_entity_class_attr_dict,
                    input_fuzzy_entity_class_list,
                    hop_pair_list,
                    intention,
                    target_entity,
                    True,
                )
                sql = self.add_user_sql(sql)
                _ = self.info_sql_run_and_parse(
                    sql, knowledge_dict, sql_entity_name_class_dict
                )
            find_employee_set = _.get("find_employee_set", set())
            diff_set = set(emp_list).difference(find_employee_set)
            if diff_set and target_entity == "Employee":
                # 存在人找不到的情况，走按姓名查人
                new_order_entity_class_list = ["Employee"]
                new_entity_class_info_set_dict = {
                    "Employee": entity_class_info_set_dict["Employee"]
                }
                undone_entity_class_attr_dict = {}
                input_fuzzy_entity_class_list = []
                sql = self.make_kbqa_sql(
                    new_order_entity_class_list,
                    new_entity_class_info_set_dict,
                    undone_entity_class_attr_dict,
                    input_fuzzy_entity_class_list,
                    hop_pair_list,
                    intention,
                    target_entity,
                    False,
                )
                sql = self.add_user_sql(sql)
                _ = self.info_sql_run_and_parse(
                    sql, knowledge_dict, sql_entity_name_class_dict
                )
        if not _:
            result_dict["kbqa_flag"] = 0
            return result_dict

        if not kbqa_flag:
            result_dict = self.cal_faq(_, ori_user_code, order_line_entity_info_list)
        else:
            result_dict = self.cal_kbqa(
                _,
                ori_user_code,
                org_list,
                dep_list,
                pos_list,
                key_list,
                input_odp_class_set,
                input_odp_set,
            )
        result_dict["kbqa_flag"] = kbqa_flag

        return result_dict
