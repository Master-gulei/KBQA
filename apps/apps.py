# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei

import time
from apps.kg_recall import KGRecall
from apps.nlg import Nlg


class GraphQA:
    def __init__(
        self,
        db,
        space,
        jieba,
        get_embedding_config,
        get_milvus_config,
        get_faq_answer_config,
        get_permission_config,
        myLogger,
        stop_words_list,
        tree_dict,
    ):
        """
        初始化
        :param db: nebula对象
        :param space: 图空间
        """
        self.log = myLogger(__file__)
        self.log.info(f"GraphQA init : db:{db},space:{space}")
        self.kg_recall_model = KGRecall(
            db,
            space,
            jieba,
            self.log,
            stop_words_list,
            tree_dict,
            get_permission_config,
        )
        self.nlg_model = Nlg(
            get_embedding_config, get_milvus_config, get_faq_answer_config, self.log
        )

    def run(self, input_info):
        """
        QA执行主函数
        :param input_info:
        :return:
        """
        # kg召回
        start_time = time.time()
        self.log.info(f"----call kg_recall_model input----: {input_info}")
        kg_result = self.kg_recall_model.run(input_info)
        end_time = time.time()
        time_interval = end_time - start_time
        self.log.info(f"call kg_recall_model cost: {str(time_interval)}秒")
        start_time = time.time()
        result = self.nlg_model.run(input_info, kg_result)
        end_time = time.time()
        time_interval = end_time - start_time
        self.log.info(f"call nlg cost: {str(time_interval)}秒")
        self.log.info(f"result: {str(result)}")
        return result
