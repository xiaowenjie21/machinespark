# 不使用python做分类预测

from pyspark import SparkContext
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Tokenizer
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import Row
from pyspark.sql import SQLContext


class RawDataRecord(object):
    def __init__(self, category, text):
        pass

    def __call__(self, category, text):
        self.category = category
        self.text = text
        return {
            'category': category,
            'text': text
        }


def split_word(path = 'd:/temp'):
    sc = SparkContext('local[4]', 'app')
    rdd = sc.textFile('d:/temp')
    files = rdd.map(lambda i: i).take(20)
    print('files: ', files)

    with open('d:/temp/news_sohusite_xml.dat', 'rb') as f:
        print(f.readlines())


def category_model(*args):
    sc = SparkContext('local[4]', 'app')
    sql = SQLContext(sc)
    srcRDD = sc.textFile('E:\spark-test-py\machinespark\spark-createTag\sougou_all')\
               .map(lambda x: Row(category = x.split(',')[0], text = x.split(',')[1]))
    # 随机划分70%数据作为训练集 30%数据作为测试集
    training, test = srcRDD.randomSplit([0.7, 0.3])
    trainingDF = training.toDF()
    testDF = test.toDF()

    # 将训练词转数组
    tokenizer = Tokenizer(inputCol = 'text', outputCol = 'words')
    wordsData = tokenizer.transform(trainingDF)
    print('output1 {}'.format(wordsData.select("category", "text", "words").take(1)))

    # 计算词频 使用哈希TF
    hashingTF = HashingTF(numFeatures = 500000, inputCol = 'words', outputCol = 'rawFeatures')
    featurizedData = hashingTF.transform(wordsData)

    # 计算每个词的TF-IDF
    idf = IDF(inputCol = 'rawFeatures', outputCol = 'features')
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    print('计算TF-IDF {}'.format(rescaledData.select('category', 'features').take(1)))

    # 将训练数据转换成byes格式
    trainDataRDD = rescaledData.select('category', 'features').rdd\
                               .map(lambda i: LabeledPoint(float(i.category), Vectors.dense(list(i.features))))
    print('output5 训练数据 {}'.format(trainDataRDD.take(1)))

    model = NaiveBayes.train(trainDataRDD, 1.0)



def main(*args):
    sc = SparkContext('local[2]', 'app')
    sql = SQLContext(sc)
    srcRDD = sc.textFile('d:/temp/news_sohusite_xml.dat').map(lambda x: RawDataRecord(x.split(',')[0], x.split(',')[1]))
    splits = srcRDD.randomSplit([0.7, 0.3])
    trainingDF = splits[0].toDF()
    testDF = splits[1].toDF()

    # 将词转化成数组
    tokenizer  =Tokenizer().setInputCol('text').setInputCol('words')
    wordsData = tokenizer.transform(trainingDF)
    print('output1 {}'.format(wordsData.select('category', 'text', 'words').take(1)))


    hashingTF = HashingTF().setNumFeatures(500000).setInputCol('words').setOutputCol('rawFeatures')
    featurizedData = hashingTF.transform(wordsData)
    print('output2 {}'.format(featurizedData.select('category', 'words', 'rawFeatures').take(1)))

    idf = IDF().setInputCol('rawFeatures').setOutputCol('features')
    idfModel = idf.fit(featurizedData)

    rescaledData = idfModel.transform(featurizedData)
    print('output 3{}'.format(rescaledData.select('cateogyr', 'features').take(1)))

    trainDataRdd = rescaledData.select('category', 'features').map(lambda i: LabeledPoint(i[0], list(i[1])))

    model = NaiveBayes.train(trainDataRdd, 1.0)


category_model()