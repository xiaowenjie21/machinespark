from machinespark.Model.Model_copy import Sql
import re
from sqlalchemy import select, desc, update
import jieba


class progress_videoText:
    '''处理视频文本分类任务'''
    def __init__(self):
        sql = Sql()
        self.connection = sql.connection
        self.videocache = sql.e.T_videocache
        self.category = {
            0:'汽车', 1:'互联网',2:'健康',3:'体育',4:'旅游',
            5:'教育', 6:'招聘',7:'文化',8:'军事',9:'房产',
            10:'娱乐', 12:'传媒', 13:'公益', 14:'财经'
        }

    def getStopWords(self):
        stopList = []
        for line in open("./中文停用词表.txt", encoding='gbk'):
            stopList.append(line[:len(line) - 1])

        return stopList



    def result(self):
        '''获取查询结果'''
        ins = select([self.videocache.c.title, self.videocache.c.id]).order_by(desc(self.videocache.c.create_time))
        with Sql().connection as conn:
            query_result = conn.execute(ins).fetchall()

        return query_result



    def cut_words(self, query_result):
        '''分词并写入文本, 此处使用的是视频文件分词'''
        stop_wrods = self.getStopWords()

        for q_r in query_result:
            result = q_r.title
            id = q_r.id
            cut_words = list(jieba.cut(result))
            format_cut_words = [t.strip() for t in cut_words if t not in stop_wrods and re.search('[\u4e00-\u9fa5]+', t)]
            with open('./video_title_cutword/0.txt', 'a', encoding='utf8') as f:
                f.write(str(id) + ',' + ' '.join(format_cut_words) + '\n')


    def save_category(self, path = None):
        '''使用scala预测好了将预测的结果进行保存，并更新到数据库列中'''
        path = r"E:\gwhgwghweghwhwhwhwh\output.txt" if path is None else path
        with open(path, 'r', encoding='utf8') as f:
            resutl = f.readlines()
            for r in resutl:
                print('r {}'.format(r))
                result = r.replace("(", '').replace(")", '')
                category_number = result.split(",")[0].split(".")[0]
                id = result.split(",")[1].split(".")[0]
                print('id {} c_number {}'.format(id, category_number))
                category_str = self.category[int(category_number)]
                ins = update(self.videocache).where(self.videocache.c.id == id).values(category = category_str)
                with Sql().connection as conn:
                    conn.execute(ins)



if __name__ == '__main__':
    p = progress_videoText()
    # query_result = p.result()
    # p.cut_words(query_result)
    p.save_category()







