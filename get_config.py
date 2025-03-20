# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei

from configparser import ConfigParser

config_file = "config.conf"
config = ConfigParser()
config.read("config.conf")
env = config.get("env", "env")
if not env:
    env = "prd"


def get_nebula_config():
    """
    获取适配当前环境的nebula库
    :return:
    """
    nebula_ip = config.get("nebula_" + env, "nebula_ip")
    nebula_port = config.get("nebula_" + env, "nebula_port")
    nebula_user = config.get("nebula_" + env, "nebula_user")
    nebula_password = config.get("nebula_" + env, "nebula_password")
    return nebula_ip, nebula_port, nebula_user, nebula_password


def get_nlg_config():
    """
    获取适配当前环境的大模型接口及参数
    :return:
    """
    llm_host = config.get("llm_nlg_" + env, "llm_host")
    llm_port = config.get("llm_nlg_" + env, "llm_port")
    llm_name = config.get("llm_nlg_" + env, "llm_name")
    llm_model = config.get("llm_nlg_" + env, "llm_model")
    llm_user = config.get("llm_nlg_" + env, "llm_user")
    llm_temperature = float(config.get("llm_nlg_" + env, "llm_temperature"))
    llm_max_tokens = int(config.get("llm_nlg_" + env, "llm_max_tokens"))
    timeout = int(config.get("llm_nlg_" + env, "timeout"))
    return (
        llm_host,
        llm_port,
        llm_name,
        llm_model,
        llm_user,
        llm_temperature,
        llm_max_tokens,
        timeout,
    )


def get_milvus_config():
    """
    获取milvus接口
    :return:
    """
    milvus_host = config.get("milvus_" + env, "milvus_host")
    milvus_port = config.get("milvus_" + env, "milvus_port")
    milvus_database = config.get("milvus_" + env, "milvus_database")
    return milvus_host, milvus_port, milvus_database


def get_embedding_config():
    """
    获取faq接口
    :return:
    """
    emb_url = config.get("emb_" + env, "url")
    return emb_url


def get_faq_answer_config():
    """
    获取faq接口
    :return:
    """
    faq_answer_url = config.get("faq_" + env, "url")
    return faq_answer_url


def get_permission_config():
    """
    获取适配当前环境的大模型接口及参数
    :return:
    """
    permission_url = config.get("permission_" + env, "url")
    timeout = int(config.get("permission_" + env, "timeout"))
    return (
        permission_url,
        timeout,
    )
