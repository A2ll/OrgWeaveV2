import pandas as pd
from collections import defaultdict
from tqdm import tqdm

# import json
from multi_level_analysis.analysis import Analysis
from multi_level_analysis.check import Info
from loguru import logger
import sys


class Main(Info):
    def __init__(self, id, parent):
        super().__init__(id, parent)
        self.flag = False

    def set(self):
        self.flag = True

    def main(self, file_path):
        logger.info("读取CSV文件数据到DataFrame")
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        # 检查是否存异常数据
        logger.info("异常检查")
        self.check_id(df)
        self.check_id_pid(df)
        # 处理空值
        df[self.parent] = df[self.parent].fillna("")
        logger.trace("构建数据结构准备")
        id_list = df[self.id].tolist()
        id_dict = {x: True for x in id_list}  # 用字典模拟集合,用来优化if查询速度
        parent_list = df[self.parent].tolist()
        if self.flag:
            self.check_pid(df, self.flag, id_dict, parent_list)
        else:
            df = self.check_pid(df, self.flag, id_dict, parent_list)
            id_list = df[self.id].tolist()
            id_dict = {x: True for x in id_list}  # 用字典模拟集合,用来优化if查询速度
            parent_list = df[self.parent].tolist()
        id_to_parent = {}
        id_to_children = defaultdict(list)
        for _, row in tqdm(
            df.iterrows(), desc="构建数据结构", total=len(df), ncols=100
        ):
            current_id, pid = row[self.id], row[self.parent]
            # todo 优化查询速度
            id_to_parent[current_id] = pid if pid in id_dict else None
            if pid in id_dict:
                id_to_children[pid].append(current_id)

        # todo 将推荐关系保存为json
        #  json_str = json.dumps(id_to_children,indent=4)
        #  with open('直属下级.json','w') as json_file:
        #      json_file.write(json_str)

        a = Analysis(self.id, self.parent, df)
        a.run(id_list, id_to_parent, id_to_children)
        a.out_excel("层级推荐关系")


if __name__ == "__main__":
    logger.remove()
    handler_id = logger.add(sys.stderr, level="INFO")
    logger.add("日志文件.log", level="TRACE")
    m = Main("member", "parent")
    m.main(r"测试样例\测试样例.csv")
