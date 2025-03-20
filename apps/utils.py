# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei


def extract_rel(path_list: list) -> list:
    """
    抽取路径（包括关系）实体
    :param path_list:
    :return:
    """
    path_parse_result_list = []
    if not isinstance(path_list, list):
        return path_parse_result_list
    for item_dict in path_list:
        if not item_dict:
            path_parse_result_list.append(("null", None, None))
        elif not isinstance(item_dict, dict):
            item_dict = str(item_dict)
            path_parse_result_list.append(("other", "other", {"name": item_dict}))
            continue
        try:
            if "tags" in item_dict:  # 实体
                item_dict_str = str(item_dict)
                item_dict_str = item_dict_str.replace("__NULL__", "'null'")
                item_dict = eval(item_dict_str)
                item_pro_dict = item_dict.get("tags", {})
                item_class = list(item_pro_dict.keys())[0]
                item_pro = item_pro_dict[item_class]
                path_parse_result_list.append(("tag", item_class, item_pro))
            elif "type" in item_dict:  # 关系
                item_dict_str = str(item_dict)
                item_dict_str = item_dict_str.replace("__NULL__", "'null'")
                item_dict = eval(item_dict_str)
                item_class = item_dict.get("type", {})
                item_pro = item_dict.get("props", {})
                path_parse_result_list.append(("edge", item_class, item_pro))
            else:
                item_dict = str(item_dict)
                path_parse_result_list.append(("other", "other", {"name": item_dict}))
        except:
            item_dict = str(item_dict)
            path_parse_result_list.append(("other", "other", {"name": item_dict}))
    return path_parse_result_list


def handle_properties(properties):
    properties = properties.split(",")
    properties = [i.strip() for i in properties]
    properties = ['"' + i[: i.find(":")] + '"' + i[i.find(":")] for i in properties]
    properties = "{" + ",".join(properties) + "}"
    properties = eval(properties)

    return properties


class Node:
    def __init__(self):
        self.children = dict()  # 初始化子节点
        self.isEnd = False  # isEnd 用于标记单词结束


class Trie:  # 字典树
    # 初始化字典树
    def __init__(self):
        self.root = Node()  # 初始化根节点（根节点不保存字符）

    # 向字典树中插入一个单词
    def insert(self, word: str) -> None:
        cur = self.root
        for ch in word:
            if ch not in cur.children:
                cur.children[ch] = Node()
            cur = cur.children[ch]
        cur.isEnd = True  # 单词处理完成时，将当前节点标记为单词结束

    # 查找字典树中是否存在一个单词
    def search(self, word: str) -> bool:
        cur = self.root
        for ch in word:
            if ch not in cur.children:
                return False
            cur = cur.children[ch]

        return cur is not None and cur.isEnd  # 判断当前节点是否为空，并且是否有单词结束标记

    # 深度优先遍历
    def dfs_ch(self, cur, cur_ch_str):
        stack = [(cur, cur_ch_str)]
        while stack:
            node, path = stack.pop()
            for char, child in node.children.items():
                if child.isEnd:
                    self.satis_word_list.append(path + char)
                stack.append((child, path + char))

    # 查找字典树中前缀最长匹配单词
    def startsWith(self, word: str):
        self.satis_word_list = []
        cur = self.root
        for i, ch in enumerate(word):
            if ch not in cur.children:
                cur_ch_str = word[:i]
                self.dfs_ch(cur, cur_ch_str)
                break
            cur = cur.children[ch]
        if not self.satis_word_list:
            self.dfs_ch(cur, word)


def make_trie(entity_name_set):
    entity_tree = Trie()
    for entity_name in entity_name_set:
        entity_tree.insert(entity_name)
        reversed_entity_name = entity_name[::-1]
        entity_tree.insert(reversed_entity_name)
    return entity_tree
