import os
from bs4 import BeautifulSoup
import jieba
import re

class Category_txt:
    '''搜狗文件预处理，将文档进行分类，并输出到sougou_all文件夹中'''


class Category:
    '''搜狗文本预处理，将文档分类输出到指定文件夹'''
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path


    def main(self):
        '''读取源文件，预处理并写入到sougou_all里面'''
        for i in os.listdir(self.input_path):
            with open(self.input_path + i, 'r', encoding = 'utf8') as f:
                try:
                    content = f.read()
                except Exception as e:
                    print('读取异常 跳过文档 {}'.format(e))
                    continue

                bs = BeautifulSoup(content, 'lxml')
                content = bs.find_all('content')
                url = bs.find_all('url')

                yield self.write_category(url, content)


    def write_category(self, url, content):
        '''将分类及内容写入文件中'''
        for url, content in zip(url, content):
            url_text = url.get_text()
            content_text = content.get_text()

            for k, v in dicturl.items():
                if k in url_text:
                    category = v
                    with open(self.output_path + category + '.txt', 'a', encoding='utf8') as f:
                        # 写入文件格式为 /分类, 分词1 分词2 分词3  \n ..... /0, words1 words2 words3
                        words_space = ' '.join(jieba.cut(content_text))
                        f.write(category + ',' + words_space + '\n')
                    break
                else:
                    continue


if __name__=="__main__":

    dicurl = {'auto.sohu.com':'qiche','it.sohu.com':'hulianwang','health.sohu.com':'jiankang',
              'sports.sohu.com':'tiyu','travel.sohu.com':'lvyou','learning.sohu.com':'jiaoyu',
              'career.sohu.com':'zhaopin','cul.sohu.com':'wenhua','mil.news.sohu.com':'junshi',
              'house.sohu.com':'fangchan','yule.sohu.com':'yule','women.sohu.com':'shishang',
              'media.sohu.com':'chuanmei','gongyi.sohu.com':'gongyi','2008.sohu.com':'aoyun',
              'business.sohu.com': 'shangye'}
    # 0 汽车 1 互联网 2 健康 3 体育 4 旅游 5 教育 6 招聘 7 文化 8 军事 9 房产 10 娱乐 12 传媒 13 公益 14财经
    dicturl = {'auto.sohu.com': '0', 'it.sohu.com': '1', 'health.sohu.com': '2',
              'sports.sohu.com': '3', 'travel.sohu.com': '4', 'learning.sohu.com': '5',
              'career.sohu.com': '6', 'cul.sohu.com': '7', 'mil.news.sohu.com': '8',
              'house.sohu.com': '9', 'yule.sohu.com': '10',
              'media.sohu.com': '12', 'gongyi.sohu.com': '13',
              'business.sohu.com': '14'}


    def getStopWords():
        stopList = []
        for line in open("./中文停用词表.txt", encoding='gbk'):
            stopList.append(line[:len(line) - 1])

        return stopList


    def main():

        stopList = getStopWords() # 获取停用词

        for i in os.listdir('./sougou_before2'):
            with open('./sougou_before2/' + i, 'r', encoding='utf8') as f:
                try:
                    content = f.read()
                except Exception as e:
                    # 读取异常的跳过
                    continue

                bs = BeautifulSoup(content, 'lxml')
                content = bs.find_all('content')
                url = bs.find_all('url')

                for url, content in zip(url, content):
                    url_text = url.get_text()
                    content_text = content.get_text()


                    for k, v in dicturl.items():
                        if k in url_text:
                            print('找到分类 分类是 {} url是 {}'.format(v, k))
                            # 找到分类立即写入文件 并 break
                            category = v
                            with open('./sougou_all/'+ category + '.txt', 'a', encoding='utf8') as f:
                                # 写入文件格式为 /分类, 分词1 分词2 分词3  \n ..... /0, words1 words2 words3
                                cut_content = list(jieba.cut(content_text))
                                # 去停用词 去非汉字词
                                words_space = []
                                for i in cut_content:
                                    if i not in stopList and i.strip() != '' and i != None and re.search('[\u4e00-\u9fa5]+', i):
                                        words_space.append(i)
                                if len(words_space) > 1:
                                    words_space_list = ' '.join(words_space)
                                    f.write(category + ','+ words_space_list + '\n')
                            break
                        else:
                            continue

    main()
