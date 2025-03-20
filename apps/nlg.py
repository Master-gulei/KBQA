# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei

import requests
import json
import time
from pypinyin import pinyin, Style
from collections import defaultdict
from apps.prompts import *
from pymilvus import Collection, connections


class Nlg:
    def __init__(
        self, get_embedding_config, get_milvus_config, get_faq_answer_config, log
    ):
        self.log = log
        self.milvus_host, self.milvus_port, self.milvus_database = get_milvus_config()
        self.embedding_url = get_embedding_config()
        self.faq_answer_url = get_faq_answer_config()
        self.log.info(f"faq_answer_service_url:{self.faq_answer_url}")
        self.entity_class_name_dict = {
            "Employee": "员工",
            "Department": "部门",
            "Organization": "机构",
            "Indicator": "指标",
            "Root": "根因",
            "Menu": "菜单",
            "Position": "岗位",
            "Code": "编号",
            "Keyword": "关键词",
            "Other": "其他",
        }
        self.same_name_information_stop_num = 20

    def get_employee_permission(self, user_code) -> list:
        """
        权限控制，用于检测用户权限
        :param user_code:
        :return: 权限列表
        """
        permission_dict = {"1": ["public"], "2": ["public", "permission"]}
        return permission_dict.get("1", "1")

    def make_entity_knowledge(self, knowledge_info_dict, user_permission_list):
        """
        根据用户权限加工实体的知识，把知识加工成string
        :param knowledge_info_dict:
        :param user_permission_list:
        :return:
        """
        entity_knowledge_dict = defaultdict(dict)
        extra_attr_list = ["所属直属上级部门", "所属二级上级部门", "所属直属上级机构"]  # 用于补充人员、部门的 机构和部门属性知识
        entity_relationship_dict = defaultdict(dict)
        faq_qa_dict = {}
        if not knowledge_info_dict:
            return entity_knowledge_dict, entity_relationship_dict, faq_qa_dict

        for entity_class, entity_info_dict in knowledge_info_dict.items():
            if entity_class == "FaqQuestion":
                for q, a in entity_info_dict.items():
                    faq_qa_dict[q] = a
                continue
            entity_code_str = self.entity_class_name_dict.get(entity_class, "") + "代码"
            for entity_code, info_dict in entity_info_dict.items():
                entity_name = info_dict.get("name", "")
                knowledge_str = (
                    entity_code_str + f"为'{entity_code}'的'{entity_name}'的知识信息："
                )
                for permission in user_permission_list:
                    for entity_info in info_dict.get(permission, []):
                        key, value = entity_info[0], entity_info[1]
                        if value == entity_code or value == entity_name:
                            continue
                        knowledge_str += f"{key}：{value}；"
                dept_code_list = info_dict.get("dept_code", [])
                if dept_code_list:
                    entity_relationship_dict[entity_code]["Department"] = dept_code_list
                for dept_code in dept_code_list:
                    dept_info_dict = knowledge_info_dict.get("Department", {}).get(
                        dept_code, {}
                    )
                    if dept_info_dict:
                        for permission in user_permission_list:
                            for entity_info in dept_info_dict.get(permission, []):
                                key, value = entity_info[0], entity_info[1]
                                if (
                                    value == entity_code
                                    or value == entity_name
                                    or key not in extra_attr_list
                                ):
                                    continue
                                knowledge_str += f"{key}：{value}；"
                org_code_list = info_dict.get("org_code", [])
                if org_code_list:
                    entity_relationship_dict[entity_code][
                        "Organization"
                    ] = org_code_list
                if dept_code_list and len(org_code_list) == 1:
                    for dept_code in dept_code_list:
                        entity_relationship_dict[dept_code][
                            "Organization"
                        ] = org_code_list
                for org_code in org_code_list:
                    org_info_dict = knowledge_info_dict.get("Organization", {}).get(
                        org_code, {}
                    )
                    if org_info_dict:
                        for permission in user_permission_list:
                            for entity_info in org_info_dict.get(permission, []):
                                key, value = entity_info[0], entity_info[1]
                                if (
                                    value == entity_code
                                    or value == entity_name
                                    or key not in extra_attr_list
                                ):
                                    continue
                                knowledge_str += f"{key}：{value}；"
                if knowledge_str[-1] == "；":
                    knowledge_str = knowledge_str[:-1]
                knowledge_str += "。 "
                entity_knowledge_dict[entity_class][entity_code] = [
                    knowledge_str,
                    entity_name,
                ]
        return entity_knowledge_dict, entity_relationship_dict, faq_qa_dict

    def parse_kg_result(self, user_code, kg_result):
        """
        解析kg返回的结果
        :param user_code:
        :param kg_result: 嵌套dict，格式参考kg_recall的返回
        :return:
        """
        knowledge_score_dict = kg_result["knowledge_score_dict"]
        knowledge_info_dict = kg_result["knowledge_info_dict"]
        knowledge_fuzzy_name_dict = kg_result["knowledge_fuzzy_name_dict"]

        # 使用用户名称
        user_name = kg_result["user_name"]
        user_permission_list = self.get_employee_permission(user_code)
        legal_entity_dict = defaultdict(dict)
        max_score_name_dict = defaultdict(dict)
        entity_code_class_name_dict = defaultdict(dict)
        entity_class_code_score_dict = defaultdict(dict)
        max_score_faq_question_list = []
        for entity_class, score_dict in knowledge_score_dict.items():
            if entity_class == "FaqQuestion":
                # faq取得分最高的问题
                faq_max_score = 0
                for entity_code, score_info in score_dict.items():
                    entity_name, entity_score = score_info[0], score_info[1]
                    if entity_score > faq_max_score and entity_score >= 0.9:
                        max_score_faq_question_list = [entity_name]
                    if entity_score == faq_max_score and entity_score >= 0.9:
                        max_score_faq_question_list.append(entity_name)
            for entity_code, score_info in score_dict.items():
                entity_name, entity_score = score_info[0], score_info[1]
                if entity_code != user_code:
                    entity_class_code_score_dict[entity_class][
                        entity_code
                    ] = entity_score
                entity_code_class_name_dict[entity_code][entity_class] = entity_name
                if entity_class not in max_score_name_dict:
                    max_score_name_dict[entity_class][entity_name] = entity_score
                    legal_entity_dict[entity_class][entity_name] = [entity_code]
                else:
                    if entity_name in max_score_name_dict[entity_class]:
                        if (
                            entity_score
                            > max_score_name_dict[entity_class][entity_name]
                        ):
                            max_score_name_dict[entity_class][
                                entity_name
                            ] = entity_score
                            legal_entity_dict[entity_class][entity_name] = [entity_code]
                        elif (
                            entity_score
                            == max_score_name_dict[entity_class][entity_name]
                        ):
                            legal_entity_dict[entity_class][entity_name].append(
                                entity_code
                            )
                    else:
                        max_score_name_dict[entity_class][entity_name] = entity_score
                        legal_entity_dict[entity_class][entity_name] = [entity_code]

        mid_legal_entity_dict = defaultdict(dict)
        py_entity_dict = defaultdict(dict)
        # 模糊匹配需要再次过滤
        if knowledge_fuzzy_name_dict:
            for entity_class, entity_name_code_dict in legal_entity_dict.items():
                for entity_name, entity_code_list in entity_name_code_dict.items():
                    for _, res_entity_name_set in knowledge_fuzzy_name_dict.items():
                        # 根据拼音模糊匹配
                        inp_entity_py = "".join(
                            [item[0] for item in pinyin(_, style=Style.NORMAL)]
                        )
                        entity_py = "".join(
                            [
                                item[0]
                                for item in pinyin(entity_name, style=Style.NORMAL)
                            ]
                        )
                        if inp_entity_py == entity_py:
                            for entity_code in entity_code_list:
                                if _ not in py_entity_dict[entity_class]:
                                    py_entity_dict[entity_class][_] = {
                                        entity_name: [entity_code]
                                    }
                                    continue
                                if entity_name in py_entity_dict[entity_class][_]:
                                    py_entity_dict[entity_class][_][entity_name].append(
                                        entity_code
                                    )
                                else:
                                    py_entity_dict[entity_class][_].update(
                                        {entity_name: [entity_code]}
                                    )
                            continue
                        if entity_name in res_entity_name_set:
                            max_score = max(
                                [
                                    max_score_name_dict[entity_class].get(item, 0)
                                    for item in res_entity_name_set
                                ]
                            )
                            # 获取名称得分最高的实体
                            if (
                                max_score_name_dict[entity_class].get(entity_name, 0)
                                == max_score
                            ):
                                for entity_code in entity_code_list:
                                    # 获取得分最高的实体
                                    if (
                                        entity_class_code_score_dict[entity_class].get(
                                            entity_code, 0
                                        )
                                        == max_score
                                    ):
                                        if _ not in mid_legal_entity_dict[entity_class]:
                                            mid_legal_entity_dict[entity_class][_] = {
                                                entity_name: [entity_code]
                                            }
                                            continue
                                        if (
                                            entity_name
                                            in mid_legal_entity_dict[entity_class][_]
                                        ):
                                            mid_legal_entity_dict[entity_class][_][
                                                entity_name
                                            ].append(entity_code)
                                        else:
                                            mid_legal_entity_dict[entity_class][_] = {
                                                entity_name: [entity_code]
                                            }
        new_mid_legal_entity_dict = defaultdict(dict)
        if py_entity_dict:
            for entity_class, item in py_entity_dict.items():
                for _, entity_code_info_dict in item.items():
                    mid_legal_entity_dict[entity_class][_] = entity_code_info_dict
        for entity_class, item in mid_legal_entity_dict.items():
            for _, entity_code_info_dict in item.items():
                for entity_name, entity_code_list in entity_code_info_dict.items():
                    new_mid_legal_entity_dict[entity_class][
                        entity_name
                    ] = entity_code_list

        legal_entity_dict.update(new_mid_legal_entity_dict)
        (
            entity_knowledge_dict,
            entity_relationship_dict,
            faq_qa_dict,
        ) = self.make_entity_knowledge(knowledge_info_dict, user_permission_list)
        legal_entity_knowledge_dict = defaultdict(dict)
        for entity_class, entity_info_dict in legal_entity_dict.items():
            for entity_name, entity_code_list in entity_info_dict.items():
                for entity_code in entity_code_list:
                    if entity_code in entity_knowledge_dict.get(entity_class, {}):
                        legal_entity_knowledge_dict[entity_class][
                            entity_code
                        ] = entity_knowledge_dict[entity_class][entity_code]
        user_knowledge = entity_knowledge_dict.get("Employee", {}).get(user_code, [])

        legal_faq_qa_dict = {}
        if faq_qa_dict and max_score_faq_question_list:
            for q in max_score_faq_question_list:
                legal_faq_qa_dict[q] = faq_qa_dict.get(q, "")

        return (
            user_name,
            user_knowledge,
            legal_entity_knowledge_dict,
            legal_faq_qa_dict,
            entity_relationship_dict,
            entity_code_class_name_dict,
            entity_class_code_score_dict,
        )

    def get_knowledge(self, input_info, kg_result):
        """
        加工prompt
        :param input_info: 输入的识别后的结果
        :param kg_result: 嵌套dict，格式参考kg_recall的返回
        :return:
        """
        employee_list = []
        indicator_list = []
        employee_code_name_dict = {}
        faq_dict = {}
        value_flag = False
        # 判断权限
        permission = kg_result.get("permission", True)
        permission_flag = kg_result.get("permission_flag", False)
        if permission_flag and not permission:
            other_knowledge = "你好，你的查询涉及隐私，如需开通权限，请咨询：李国斌（03132342）"
            return (
                "",
                other_knowledge,
                faq_dict,
                employee_list,
                indicator_list,
                permission,
                value_flag,
            )
        kg_knowledge = kg_result.get("knowledge_info_dict")
        if not kg_knowledge:
            return (
                "",
                "",
                faq_dict,
                employee_list,
                indicator_list,
                permission,
                value_flag,
            )
        user_code = input_info.get("user_code", "")
        input_entity_info_dict = input_info.get("entity_info", {})
        target_entity = input_info.get("target_entity", "Employee")

        intention = input_info.get("intention", "")
        if intention == "count" and "count" in kg_result:
            other_knowledge = "问题结果为：" + kg_result["count"]
            return (
                "",
                other_knowledge,
                faq_dict,
                employee_list,
                indicator_list,
                permission,
                value_flag,
            )

        input_entity_info_dict_copy = input_entity_info_dict.copy()
        for entity_class, input_entity_info in input_entity_info_dict.items():
            if not input_entity_info:
                del input_entity_info_dict_copy[entity_class]
        input_entity_info_dict = input_entity_info_dict_copy
        input_employee_name_list = input_entity_info_dict.get("Employee", [[]])
        input_employee_name_list = [
            item[0] for item in input_employee_name_list if item
        ]
        input_code_list = input_entity_info_dict.get("Code", [[]])
        input_code_list = [item[0] for item in input_code_list if item]
        (
            user_name,
            user_knowledge,
            entity_knowledge_dict,
            faq_dict,
            entity_relationship_dict,
            entity_code_class_name_dict,
            entity_class_code_score_dict,
        ) = self.parse_kg_result(user_code, kg_result)

        if user_knowledge:
            user_knowledge = user_knowledge[0]
        else:
            user_knowledge = ""
        other_knowledge = ""
        done_code_list = []
        entity_class_info_num_dict = defaultdict(int)  # 超过stop数，就截断，查值类不适用
        if intention == "value":
            self.same_name_information_stop_num = 99
        for entity_class, entity_info_dict in entity_knowledge_dict.items():
            if entity_class in input_entity_info_dict:
                for entity_code, entity_value_list in entity_info_dict.items():
                    entity_knowledge, entity_name = (
                        entity_value_list[0],
                        entity_value_list[1],
                    )
                    if entity_code in input_code_list:
                        entity_class_info_num_dict[entity_class] += 1
                        if (
                            entity_class_info_num_dict[entity_class]
                            > self.same_name_information_stop_num
                            or entity_code in done_code_list
                        ):
                            continue
                        if (
                            "Employee"
                            in entity_code_class_name_dict.get(entity_code, {}).keys()
                        ):
                            employee_list.append(entity_code)
                            employee_code_name_dict[entity_code] = entity_name
                        other_knowledge += entity_knowledge + "//"
                        done_code_list.append(entity_code)
                    if (
                        entity_code == user_code
                        and user_name not in input_employee_name_list
                        and input_employee_name_list
                    ):
                        continue
                    input_entity_name_list = [
                        item[0]
                        for item in input_entity_info_dict.get(entity_class, [[]])
                        if item
                    ]
                    if entity_name in input_entity_name_list:
                        entity_class_info_num_dict[entity_class] += 1
                        if (
                            entity_class_info_num_dict[entity_class]
                            > self.same_name_information_stop_num
                        ):
                            continue
                        other_knowledge += entity_knowledge + "//"
                        done_code_list.append(entity_code)
                        if "Employee" == entity_class:
                            employee_list.append(entity_code)
                            employee_code_name_dict[entity_code] = entity_name
                    else:
                        entity_class_info_num_dict[entity_class] += 1
                        if (
                            entity_class_info_num_dict[entity_class]
                            > self.same_name_information_stop_num
                            or entity_code in done_code_list
                        ):
                            continue
                        other_knowledge += entity_knowledge + "//"
                        done_code_list.append(entity_code)
                    if "Employee" == entity_class:
                        rel_info_dict = entity_relationship_dict.get(entity_code, {})
                        for rel_class, rel_code_list in rel_info_dict.items():
                            for rel_code in rel_code_list:
                                rel_entity_value_list = entity_knowledge_dict.get(
                                    rel_class, {}
                                ).get(rel_code, [])
                                if (
                                    not rel_entity_value_list
                                    or len(rel_entity_value_list) == 2
                                ):
                                    entity_class_info_num_dict[entity_class] += 1
                                    if (
                                        entity_class_info_num_dict[entity_class]
                                        > self.same_name_information_stop_num
                                        or entity_code in done_code_list
                                    ):
                                        continue
                                    other_knowledge += entity_knowledge + "//"
                                    done_code_list.append(entity_code)
                                    employee_list.append(entity_code)
                                    employee_code_name_dict[entity_code] = entity_name
            elif input_code_list:
                # 没有名称，检验下code
                for entity_code, entity_value_list in entity_info_dict.items():
                    entity_knowledge, entity_name = (
                        entity_value_list[0],
                        entity_value_list[1],
                    )
                    if entity_code in input_code_list:
                        entity_class_info_num_dict[entity_class] += 1
                        if (
                            entity_class_info_num_dict[entity_class]
                            > self.same_name_information_stop_num
                            or entity_code in done_code_list
                        ):
                            continue
                        other_knowledge += entity_knowledge + "//"
                        done_code_list.append(entity_code)
                        if (
                            "Employee"
                            in entity_code_class_name_dict.get(entity_code, {}).keys()
                        ):
                            employee_list.append(entity_code)
                            employee_code_name_dict[entity_code] = entity_name
                    else:
                        continue
            else:
                # 在输入中没有的，去实体类型中取得分最高的
                if target_entity == entity_class:
                    entity_class_core_list = list(
                        entity_class_code_score_dict.get(entity_class, {}).values()
                    )
                    max_score = (
                        max(entity_class_core_list) if entity_class_core_list else 0
                    )
                    for entity_code, entity_value_list in entity_info_dict.items():
                        entity_score = entity_class_code_score_dict.get(
                            entity_class, {}
                        ).get(entity_code, 0)
                        entity_knowledge, entity_name = (
                            entity_value_list[0],
                            entity_value_list[1],
                        )
                        if entity_score != max_score and intention == "info":
                            continue
                        if (
                            entity_code == user_code
                            and user_name not in input_employee_name_list
                            and user_code not in input_code_list
                            and intention == "info"
                        ):
                            continue
                        entity_class_info_num_dict[entity_class] += 1
                        if entity_code in done_code_list:
                            continue
                        if (
                            entity_class_info_num_dict[entity_class]
                            > self.same_name_information_stop_num
                            and intention == "info"
                        ):
                            continue
                        if entity_code == user_code:
                            continue
                        other_knowledge += entity_knowledge + "//"
                        done_code_list.append(entity_code)
                        employee_code_name_dict[entity_code] = entity_name
                        employee_list.append(entity_code)
        if not employee_list and target_entity == "Employee":
            employee_code_name_dict[user_code] = user_name

        # 指标查值场景
        org_dict = entity_knowledge_dict.get("Organization", {})
        if intention == "value" and target_entity == "Indicator" and org_dict:
            value_flag = True
            indicator_name_list = input_entity_info_dict.get("Indicator", [["", 0]])
            input_indicator_name = indicator_name_list[0][0]
            indicator_list.append(input_indicator_name)
            indicator_info_dict = entity_knowledge_dict.get("Indicator")
            for entity_code, entity_value_list in indicator_info_dict.items():
                ys_indicator = entity_value_list[1]
                if ys_indicator not in indicator_list:
                    indicator_list.append(ys_indicator)

        return (
            user_knowledge,
            other_knowledge,
            faq_dict,
            employee_list,
            indicator_list,
            permission,
            value_flag,
        )

    def text_to_vector_api_load(self, payload):
        headers = {
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(
                url=self.embedding_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=1,
            )
            return response.json()
        except Exception as e:
            self.log.info(f"call embedding api error: {e}")
            return {}

    def question2embedding(self, strText: str):
        payload = {"input": strText, "model": "bge-m3-yto"}
        data = self.text_to_vector_api_load(payload)
        if data:
            embedding_data = data.get("data", [])
            if embedding_data:
                return embedding_data[0]["embedding"]
        return False

    def RecommendQuestionByMilvus(self, question):
        # 生成向量
        topN = 2
        vectorstore = self.question2embedding(question)
        try:
            connections.connect(
                alias="default", host=self.milvus_host, port=self.milvus_port
            )
        except Exception as e:
            self.log.info(f"milvus connect error: {e}")

        collection = Collection("bot_faq_new")
        collection.load()
        search_params = {
            "metric_type": "COSINE",
            "offset": 0,
            "ignore_growing": False,
            "params": {"nprobe": 10},
        }
        results = collection.search(
            data=[vectorstore],
            anns_field="problemVector",
            # the sum of `offset` in `param` and `limit`
            # should be less than 16384.
            param=search_params,
            limit=int(topN),
            expr="botId==2 and status==1 and array_contains_any(channels,['jsc'])",
            # set the names of the fields you want to
            # retrieve from the search result.
            output_fields=["faqPsId", "problem", "status", "channels"],
            consistency_level="Strong",
        )

        maxValue, minValue = 2, 0.8
        ContentList = []
        done_list = []
        if len(results) > 0:
            for hit in results[0]:
                score = hit.distance
                faqPsId = hit.entity.get("faqPsId")
                problem = hit.entity.get("problem")
                status = hit.entity.get("status")
                channels = hit.entity.get("channels")
                if problem in done_list:
                    continue
                done_list.append(problem)
                if maxValue >= score >= minValue:
                    ContentList.append([problem, score, faqPsId, status, channels])

        # 去重
        self.log.info(f"call milvus api result: {str(ContentList)}")
        # 释放连接
        connections.disconnect("default")
        if ContentList:
            return [
                {
                    "question": item[0],
                    "score": item[1],
                    "faqPsId": item[2],
                    "status": item[3],
                    "channels": item[4],
                }
                for item in ContentList
            ]
        return []

    def get_faq_answer(self, faq_ps_id, channel):
        answer_type = ""
        answer = ""
        headers = {
            "Content-Type": "application/json",
        }
        try:
            url = f"{self.faq_answer_url}/dify-tools/faq/getAnswer?faqPsId={faq_ps_id}&channel={channel}"
            response = requests.post(url=url, headers=headers)
            data = response.json()
            answer_type = data.get("data", {}).get("type", "")
            answer = data.get("data", {}).get("answer", "")
            self.log.info(f"call faq answer api result: {str(data)}")
        except Exception as e:
            self.log.info(f"call faq answer api error: {e}")

        return answer_type, answer

    def run(self, input_info, kg_result):
        """
        答案生成执行函数
        :param input_info: 输入的识别后的结果
        :param kg_result: 嵌套dict，格式参考kg_recall的返回
        :return:
        """
        result = {}
        call_rag = True
        (
            user_knowledge,
            target_knowledge,
            faq_dict,
            employee_list,
            indicator_list,
            permission,
            value_flag,
        ) = self.get_knowledge(input_info, kg_result)

        if value_flag:
            call_rag = False
            result["user_knowledge"] = user_knowledge
            result["target_knowledge"] = target_knowledge
            result["faq_knowledge_dict"] = faq_dict
            result["employee_list"] = employee_list
            result["indicator_list"] = indicator_list
            result["call_rag"] = call_rag
            return result

        kbqa_flag = kg_result.get("kbqa_flag", 1)

        self.log.info(f"call kbqa faq_dict: {str(faq_dict)}")
        question = input_info.get("question", "")

        faq_score_q_dict = defaultdict(list)
        faq_q_score_dict = {}
        kg_faq_q_list = []

        try:
            data = self.RecommendQuestionByMilvus(question)
            for item in data:
                question = item.get("question", "")
                score = item.get("score", "")
                faqPsId = item.get("faqPsId", "")
                channels = item.get("channels", "")
                channels = "jsc"
                answer_type, answer = self.get_faq_answer(faqPsId, channels)
                if question in faq_dict:
                    kg_faq_q_list.append(question)
                if item and question not in faq_dict:
                    faq_dict[question] = [answer, score]
                if score >= 0.9:
                    call_rag = False
                if score >= 0.999999:
                    score = 1
                faq_score_q_dict[score].append(question)
                faq_q_score_dict[question] = score
            self.log.info(f"get faq result: {str(data)}")
        except Exception as e:
            self.log.info(f"get faq error: {e}")

        if kbqa_flag == 1 and target_knowledge:
            call_rag = False

        if faq_score_q_dict:
            faq_max_score = max(faq_score_q_dict.keys())
            if not call_rag and not target_knowledge:
                question = faq_score_q_dict[faq_max_score][0]
                faq_dict = {question: faq_dict[question]}
            if faq_max_score == 1:
                call_rag = False
                target_knowledge = ""
                user_knowledge = ""
                employee_list = []
                question = faq_score_q_dict[faq_max_score][0]
                faq_dict = {question: faq_dict[question]}
            else:
                if kg_faq_q_list:
                    both_max_score = max(
                        [faq_q_score_dict[item] for item in kg_faq_q_list]
                    )
                else:
                    both_max_score = faq_max_score
                question = faq_score_q_dict[both_max_score][0]
                faq_dict = {question: faq_dict[question]}
                if both_max_score > 0.93:
                    call_rag = False
                    user_knowledge = ""
                    target_knowledge = ""
                    employee_list = []

        if not permission and kbqa_flag == 1:
            faq_dict = {}
            call_rag = False
        result["user_knowledge"] = user_knowledge
        result["target_knowledge"] = target_knowledge
        result["faq_knowledge_dict"] = faq_dict
        result["employee_list"] = employee_list
        result["indicator_list"] = indicator_list
        result["call_rag"] = call_rag
        return result
