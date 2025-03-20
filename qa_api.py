# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei

import re
import time
import uvicorn
import jieba
import json
from apps.database import Database
import pandas as pd
from collections import defaultdict
from fastapi import FastAPI, Request
from apps.apps import GraphQA
from fastapi.responses import JSONResponse
from logs.my_log import myLogger
from apps.utils import make_trie
from get_config import (
    get_nebula_config,
    get_nlg_config,
    get_embedding_config,
    get_milvus_config,
    get_faq_answer_config,
    get_permission_config,
)

log = myLogger(__file__)


def get_question_entity(question, file_entity_dict, same_dict, other_name_class_dict):
    done_set = set()
    add_entity_info_dict = {}
    haved_set = set()
    params_dict = defaultdict(list)
    for entity_class, entity_set in file_entity_dict.items():
        for entity in entity_set:
            if entity in question and entity not in done_set:
                done_set.add(entity)
                haved_set.add(entity)
                add_entity_info_dict[entity] = entity_class

    for other in other_name_class_dict:
        other_class = other_name_class_dict[other]
        if other in question and other not in done_set:
            done_set.add(other)
            haved_set.add(other)
            add_entity_info_dict[other] = other_class

    # 规整
    del_entity_set = set()
    for entity in add_entity_info_dict:
        for item in haved_set:
            if entity == item:
                continue
            if entity in item:
                del_entity_set.add(entity)
    for entity in del_entity_set:
        del add_entity_info_dict[entity]

    for entity in add_entity_info_dict:
        entity_class = add_entity_info_dict[entity]
        params_dict[entity_class].append([entity, 0])

    return params_dict


def Api():
    app = FastAPI()
    nebula_ip, nebula_port, nebula_user, nebula_password = get_nebula_config()

    __jieba__ = jieba
    __jieba__.initialize()

    stop_words_set = set()
    with open("data/baidu_stopwords.txt", "r", encoding="utf-8") as f:
        for line in f:
            stop_words_set.add(line.strip())
    stop_words_list = list(stop_words_set)

    dept_name_set = set()
    pos_name_set = set()

    dept_file = "data/all_department.csv"
    df = pd.read_csv(dept_file)
    for index, row in df.iterrows():
        name = row["部门名称"]
        dept_name_set.add(name)

    pos_file = f"data/all_position.csv"
    df = pd.read_csv(pos_file)
    for index, row in df.iterrows():
        name = row["岗位名称"]
        pos_name_set.add(name)

    dept_tree = make_trie(dept_name_set)
    pos_tree = make_trie(pos_name_set)
    tree_dict = {"Department": dept_tree, "Position": pos_tree}

    file_entity_dict = defaultdict(set)
    same_dict = defaultdict(set)
    other_name_class_dict = {}
    f = open("data/faq_entity.txt", "r", encoding="gbk")
    for line in f.readlines():
        if not line:
            continue
        if len(line) < 2:
            continue
        split_list = line.split("：")
        entity_class, entity_value_str = split_list[0], split_list[-1]
        for item in re.split(r"[、，]+", entity_value_str):
            item = item.strip()
            item = item.replace("\n", "")
            if not item:
                continue
            result = re.findall(r"（([^）]+)", item)
            if result:
                ori_item = item.replace("（" + result[0] + "）", "")
                file_entity_dict[entity_class].add(ori_item)
                file_entity_dict[entity_class].add(result[0])
                same_dict[ori_item].add(result[0])
                same_dict[result[0]].add(ori_item)
                continue
            file_entity_dict[entity_class].add(item)

    # 菜单
    menu_rel_file_path = "data/ods_gpt_kpi_statement_relationship_dd_202405151521.csv"
    df = pd.read_csv(menu_rel_file_path, dtype={"index_relation_id": int})
    for index, row in df.iterrows():
        menu_name = row["state_name"]
        other_name_class_dict[menu_name] = "Menu"

    # 指标实体和关系
    indicator_attr_file_path = (
        "data/config_index_base_info_a_202405091957.csv"  # 指标属性数据
    )
    df = pd.read_csv(
        indicator_attr_file_path,
        dtype={"index_code": str, "modifier": str, "del_flag": str},
    )
    for index, row in df.iterrows():
        index_name = row["index_name"]
        other_name_class_dict[index_name] = "Indicator"

    @app.post("/graphrag/qa/v1")
    async def graph_qa_api(request: Request):
        """
        input param list:request
        :return:
        """
        try:
            db = Database(nebula_ip, nebula_port, nebula_user, nebula_password)
        except Exception as e:
            log.info(f"db init error:{e}")
            return JSONResponse(content={})
        result = ""
        try:
            try:
                body = await request.json()
                body = body.get("entity_info", "{}")
                body = eval(body)
            except Exception as e:
                msg = "input params error"
                data = {
                    "result": result,
                    "code": "1002",
                    "msg": msg,
                }
                log.info(f"msg:{e}")
                db.session.release()
                db.connection_pool.close()
                return JSONResponse(content=data)

            requestId = body.get("requestId", "")
            log.info(f"----requestId:{requestId}---- qa start")

            # if not requestId:
            #     msg = "param: lost must param"
            #     data = {
            #         "result": result,
            #         "code": "1003",
            #         "msg": msg,
            #     }
            #     log.info(f"--requestId:{requestId}--; msg:{msg}")
            #     db.session.release()
            #     return JSONResponse(content=data)

            info = body.get("info", {})
            if not info:
                msg = "param: lost must param"
                data = {
                    "result": result,
                    "code": "1003",
                    "msg": msg,
                }
                log.info(f"--requestId:{requestId}--; msg:{msg}; body:{body}")
                db.session.release()
                db.connection_pool.close()
                return JSONResponse(content=data)

            try:
                if isinstance(info, dict):
                    space = "auto_employee_graph"
                    gqa = GraphQA(
                        db,
                        space,
                        __jieba__,
                        get_embedding_config,
                        get_milvus_config,
                        get_faq_answer_config,
                        get_permission_config,
                        myLogger,
                        stop_words_list,
                        tree_dict,
                    )
                    result = gqa.run(info)
                    msg = "graph qa app run success"
                    data = {
                        "result": result,
                        "code": "200",
                        "msg": msg,
                    }
                else:
                    msg = "graph qa param type error"
                    data = {
                        "result": result,
                        "code": "1004",
                        "msg": msg,
                    }
                    log.info(f"--requestId:{requestId}--; msg:{msg}")
            except Exception as e:
                msg = "graph qa app run failed"
                data = {
                    "result": result,
                    "code": "1005",
                    "msg": msg,
                }
                log.info(f"--requestId:{requestId}--; msg:{msg}")
            log.info(f"--requestId:{requestId}-- qa end")
        except Exception as e:
            msg = "api run error"
            data = {
                "result": result,
                "code": "1001",
                "msg": msg,
            }
            log.info(f"msg:{e}")

        db.session.release()
        db.connection_pool.close()

        return JSONResponse(content=data)

    return app


if __name__ == "__main__":
    app = Api()
    # 启动FastAPI应用，监听9102端口
    uvicorn.run(app=app, host="10.7.40.125", port=9102)
