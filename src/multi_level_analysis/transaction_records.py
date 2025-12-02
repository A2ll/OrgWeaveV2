import pandas as pd
from loguru import logger
from tqdm import tqdm
from decimal import Decimal
"""
根据推荐关系，计算账号所有下级账号的充值、提现金额
"""

class Cest():
    def __init__(self):
        self.id = 'id'
        self.pid = 'pid'
        self.recharge = 'recharge'
        self.withdraw = 'withdraw'

    def calculate_sums_efficient(self,df):
        logger.info('数据初始化')
        # 处理数据空值
        df[self.recharge] = df[self.recharge].replace("", pd.NA)
        df[self.recharge] = df[self.recharge].fillna('0')
        df[self.withdraw] = df[self.withdraw].replace("", pd.NA)
        df[self.withdraw] = df[self.withdraw].fillna('0')
        
        # 将字符串转换为数值
        df[self.recharge] = df[self.recharge].apply(Decimal)
        df[self.withdraw] = df[self.withdraw].apply(Decimal)
        
        # 构建父子关系字典
        logger.info('构建父子关系字典')
        parent_to_children = df.groupby(self.pid)[self.id].apply(list).to_dict()
        
        # 构建id到recharge和withdraw的映射
        logger.info('构建id到recharge和withdraw的映射')
        id_to_recharge = df.set_index(self.id)[self.recharge].to_dict()
        id_to_withdraw = df.set_index(self.id)[self.withdraw].to_dict()
        
        # 初始化结果字典
        logger.info('初始化结果字典')
        sum_recharge = {id: Decimal('0.0') for id in df[self.id]}
        sum_withdraw = {id: Decimal('0.0') for id in df[self.id]}
        
        # 使用广度优先搜索计算每个节点的下级总和
        logger.info('使用广度优先搜索计算每个节点的下级总和')
        for id in tqdm(df[self.id],desc='分析中',total=len(df),ncols=100):
            queue = [id]
            while queue:
                current_id = queue.pop(0)
                children = parent_to_children.get(current_id, [])
                queue.extend(children)
                if current_id != id:
                    sum_recharge[id] += id_to_recharge[current_id]
                    sum_withdraw[id] += id_to_withdraw[current_id]
        
        # 将结果添加到DataFrame
        logger.info('将结果添加到DataFrame')
        df['所属下级总充值'] = df[self.id].map(sum_recharge)
        df['所属下级总提现'] = df[self.id].map(sum_withdraw)
        
        df.to_csv('out.csv', index=False)
        return df

if __name__ == '__main__':
    logger.info('读取CSV文件数据到DataFrame')
    df = pd.read_csv(r'.csv', dtype=str, keep_default_na=False)
    a = Cest()
    a.calculate_sums_efficient(df)
