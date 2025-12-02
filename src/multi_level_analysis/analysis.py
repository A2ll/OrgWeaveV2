from collections import deque
from multi_level_analysis.check import Info
from loguru import logger


class Analysis(Info):
    def __init__(self,id, pid, df):
        super().__init__(id,pid)
        self.df = df
        self.id_to_parent = {}
        
    def get_relation(self,id):
        result = []
        current = id
        while current in self.id_to_parent and self.id_to_parent[current]:
            result.append(str(current))
            current = self.id_to_parent[current]
        return ','.join(result[::-1])

    def run(self,id_list, id_to_parent, id_to_children):
        logger.info('计算所在层级')
        levels = {}
        queue = deque()
        for id in id_list:
            if not id_to_parent[id]:
                levels[id] = 1
                queue.append(id)

        while queue:
            current_id = queue.popleft()
            for child in id_to_children.get(current_id, []):
                levels[child] = levels[current_id] + 1
                queue.append(child)

        logger.info('计算总下级数量和子树深度')
        processing_order = []
        visited = set()

        for id in id_list:
            if id not in visited:
                stack = [(id, False)]
                while stack:
                    node, processed = stack.pop()
                    if processed:
                        processing_order.append(node)
                        continue
                    if node in visited:
                        continue
                    visited.add(node)
                    stack.append((node, True))
                    for child in reversed(id_to_children.get(node, [])):
                        if child not in visited:
                            stack.append((child, False))

        logger.trace('计算总下级数')
        total_children = {}
        for node in processing_order:
            total_children[node] = sum(1 + total_children.get(child, 0) 
                                for child in id_to_children.get(node, []))

        logger.trace('计算子树深度')
        subtree_depth = {}
        for node in processing_order:
            max_depth = max((subtree_depth.get(child, 0) 
                            for child in id_to_children.get(node, [])), default=0)
            subtree_depth[node] = max_depth + 1 if id_to_children[node] else 0

        logger.info('添加计算结果到DataFrame')
        self.df['所在层数'] = self.df[self.id].map(levels)
        self.df['直接下级数'] = self.df[self.id].apply(lambda x: len(id_to_children.get(x, [])))
        self.df['总下级数'] = self.df[self.id].map(total_children)
        self.df['下级层数'] = self.df[self.id].map(subtree_depth)
        logger.info('添加推荐关系到DataFrame')
        self.df['推荐关系'] = self.df[self.id].apply(
            lambda x: ','.join(
                (lambda m: (lambda f: f(f, m, []))(lambda f, c, r: 
                    r.insert(0, str(c)) or (f(f, id_to_parent[c], r) if c in id_to_parent and id_to_parent[c] else r)
                ))(x)[::-1]
            )
        )
        return self.df
        
    def out_csv(self, out_path):
        logger.info('创建CSV文件')
        self.df.to_csv(out_path + '.csv', index=False)
            
    def out_excel(self, out_path):
        logger.info('创建CSV文件')
        self.df.to_csv(out_path + '.csv.bak', index=False)
        try:
            logger.info('创建Excel文件')
            self.df.to_excel(out_path + '.xlsx', index=False)
        except Exception as error:
            logger.error(error)
    