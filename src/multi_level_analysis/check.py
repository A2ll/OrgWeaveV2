from loguru import logger
import pandas as pd

class Info(object):
    def __init__(self, id, parent):
        self.id = id
        self.parent = parent
      
      
    def check_id(self,df):
        logger.trace('检查数据重复ID')
        if df[self.id].duplicated().any():
            dup_ids = df[df[self.id].duplicated()][self.id].unique()
            logger.error(f"发现重复ID: {', '.join(dup_ids)}")
            exit()
    
    def check_id_pid(self,df):
        logger.trace('检查ID与推荐ID是否有层级关系')
        mismatches = df[df[self.id] == df[self.parent]]
        num = len(mismatches)
        if num !=0:
            logger.error("发现问题ID:")
            logger.error(mismatches)
            exit()

    def check_pid(self,df,flag,id_dict,parent_list):
        logger.trace('检查层级推荐关系是否完整')
        parent_isolated = []
        for pid_key in parent_list:
            if pid_key not in id_dict and str(pid_key) != '':
                parent_isolated.append(pid_key)
        if len(parent_isolated) != 0:
            if flag:
                parent_isolated = list(dict.fromkeys(parent_isolated))
                logger.warning(f"发现孤立上级: {len(parent_isolated)}个")
                logger.trace(parent_isolated)
            else:
                # 构造新的 DataFrame，id 列来自列表，其他列填充 'NULL'
                parent_isolated = list(dict.fromkeys(parent_isolated))
                logger.info(f"发现孤立上级: {len(parent_isolated)}个,将孤立上级加入到数据中进行层级计算")
                logger.trace(parent_isolated)
                new_data = pd.DataFrame({self.id: parent_isolated})
                for col in df.columns:
                    if col != self.id:
                        new_data[col] = ''  # 其他列填充 ''
                # 合并原 DataFrame 和新数据
                # final_df = pd.concat([df, new_data], ignore_index=True)
                final_df = pd.concat([df, new_data])
                return final_df
