import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from loguru import logger
from datetime import datetime


class To_Mysql(object):
    def __init__(self):
        self.db_config = {
            'host': '192.168.89.139',
            'user': 'root',
            'password': '123456',
            'database': 'GDJM'
        }
        self.flag = 12116432
        self.sql = 'SELECT * FROM `充值管理_info` ORDER BY 订单号'
        
    def to_excel(self,query,conn,num):
        logger.info(query)
        start_time = datetime.now()
        df = pd.read_sql(query, conn)
        logger.info(f"查询完成，获取到 {len(df)} 行数据，耗时: {datetime.now() - start_time}")
        
        # !将DataFrame写入Excel文件
        output_file = f'平台用户充值记录-{num}.xlsx'
        start_time = datetime.now()
        df.to_excel(output_file, index=False)
        logger.success(f"数据已成功导出到 {output_file}，耗时: {datetime.now() - start_time}")

    def _test(self):
        try:
            # 建立数据库连接
            logger.info("尝试连接数据库...")
            conn = mysql.connector.connect(**self.db_config)
            
            # cursor = conn.cursor()
            logger.info("数据库连接成功！")

            # 先执行表结构SQL
            i = 0
            num = 1
            # ! 查询数据量
            while i < self.flag:
                #! 查询语句
                query = self.sql + f' LIMIT {i}, 1000000'
                self.to_excel(query,conn,num)
                i = i + 1000000
                num = num + 1

        except mysql.connector.Error as err:
            # 错误处理
            logger.error(f"数据库错误: {err}")
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("请检查用户名/密码")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("数据库不存在")
            else:
                logger.error(f"未知错误: {err}")
            if "conn" in locals():
                conn.rollback()

        finally:
            # 清理资源
            if "conn" in locals():
                conn.close()
            logger.trace("数据库连接已关闭。")

if __name__ == "__main__":
    ins_sql = To_Mysql()
    ins_sql._test()