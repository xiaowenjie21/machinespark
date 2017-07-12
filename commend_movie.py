from pyspark import SparkContext
from operator import add
from pyspark.mllib.recommendation import ALS, Rating
import numpy as np


class Recommend:
    '''Recommand Data in Spark'''
    def __init__(self):
        self.u_data_path = r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.data'
        self.u_item_path = r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.item'
        self.sc = SparkContext('local[4]', 'app_recommend')

    def info_message(self):
        '''infomation: userID, movieID, start, timestamp'''
        file_data = self.sc.textFile(self.u_data_path)
        print('file data first {}'.format(file_data.first()))

    def split_tab(self):
        file_data = self.sc.textFile(self.u_data_path)
        u_data_field = file_data.map(lambda i: i.split('\t'))
        print('u_data_field {}'.format(u_data_field.take(5)))


    def split_tab_require(self):
        '''split tab of u data by my require'''
        rawData = self.sc.textFile(self.u_data_path)
        u_data_field = rawData.map(lambda i: i.split('\t')[:3])
        print('u_data_field {}'.format(u_data_field.first()))

    def mlib_als(self):
        rawData = self.sc.textFile(self.u_data_path)
        u_data_field = rawData.map(lambda i: Rating(int(i.split('\t')[0]),
                                                    int(i.split('\t')[1]),
                                                    float(i.split('\t')[2])))

        print('mlib als u_data_field {}'.format(u_data_field.first()))
        rating = u_data_field
        return rating

    # ------------------model -------------------

    def model(self, rating):
        '''build this model
        @:rating user project rating
        '''
        model = ALS.train(rating, 50, 10, 0.01)
        print('user features {}'.format(model.userFeatures().take(1)))
        print('project features {}'.format(model.productFeatures().take(1)))
        print('---beigin keyby--')
        movieByUser = rating.keyBy(lambda x: x.user).lookup(789) # find all movie of userID is 789
        print('movideByUser len {}'.format(len(movieByUser))) # return 33
        print('---end keyby---')

        return model

    def sort_movieForUser(self, rating):
        '''sort movide for user 
           example @@@@ 
                r = Recommend()
                r.split_tab_require()
                rating = r.mlib_als()
                r.sort_movieForUser(rating)
                进行验证
            :return movieByUser (default 789)
        '''

        movie_data = self.get_movie_data()
        movieByUser = rating.keyBy(lambda x: x.user).lookup(789)
        the_movieByUser = map(lambda i: (i.rating, movie_data[i.product]), movieByUser)
        movieLikeForUser = sorted(the_movieByUser, key = lambda i: i[0], reverse= True)
        print('sort movide for user {}'.format(movieLikeForUser))

        return movieByUser


    # --------------- part. predict --------------
    def predict(self, model):
        '''use model predict this value by userid, movideid'''
        pred_value = model.predict(789, 123)
        print('pred value {}'.format(pred_value))

    def recommand_num(self, model):
        ''' use model remmend this before 5 '''
        product_value = model.recommendProducts(789, 10)
        print('product value recommend: {}\n'.format(product_value))


    def predict_rdd(self, model):
        rawData = self.sc.textFile(self.u_data_path)
        u_data_field = rawData.map(lambda i: i.split('\t'))
        more_data = u_data_field.map(lambda i: (i[0], i[1]))
        pred_value = model.predictAll(more_data)
        print('pred value {}'.format(pred_value.take(5)))


    def get_movie_data(self):
        '''no implement the score of model'''
        u_item_data = self.sc.textFile(self.u_item_path)
        print('u_item_data {}'.format(u_item_data.first()))
        movie_field = u_item_data.map(lambda i: i.split('|'))
        movide_data = movie_field.map(lambda i: i[1])
        print('movide_data {}'.format(movide_data.first()))

        movie_data2 = u_item_data.map(lambda i: i.split("|")).map(lambda i: (int(i[0]), (i[1]))).collectAsMap()
        print('movie data2 ID 504 {}'.format(movie_data2.get(504)))

        return movie_data2



def cos_like(x, y):  # 计算余弦相似度函数
    tx = np.array(x)
    ty = np.array(y)
    cos1 = np.sum(tx * ty)
    cos21 = np.sqrt(sum(tx ** 2))
    cos22 = np.sqrt(sum(ty ** 2))
    return cos1 / float(cos21 * cos22)



class Recommend_cos(Recommend):
    def __str__(self):
        print('use cos recommend')



    def cos_recom_test(self):
        self.split_tab_require()
        rating = self.mlib_als()
        model = self.model(rating)

        itemFactor = model.productFeatures().lookup(567)[0]
        print('itemFactor: ', itemFactor)
        print(cos_like(itemFactor, itemFactor))


    def cos_recom(self):
        '''计算每个movie id的相关度
           example r = Recommend_cos()
                   r.cos_recom()
           return [(567, 1.0000000000000002)...] # 相似度
        '''
        self.split_tab_require()
        rating = self.mlib_als()
        model = self.model(rating)
        itemFactor = model.productFeatures().lookup(691)[0] # 获取uid 为567 的 向量化值
        sims = model.productFeatures().map(lambda k_v: (k_v[0], cos_like(k_v[1], itemFactor)))
        print(sims.takeOrdered(10, lambda i: -i[1]))
        return sims

    def valid_model(self):
        '''检验模型效果
           :return 被推荐的所有的movie title 与 其的相似度
        '''
        movie_data = self.sc.textFile(r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.item')
        movie_item = movie_data.map(lambda i:i.split('|')).map(lambda i: (int(i[0]),i[1])).collectAsMap()
        print('123 movie title: {}'.format(movie_item.get(123)))

        sims = self.cos_recom()
        recommend_title = sims.map(lambda i: (movie_item.get(int(i[0])), i[1])).takeOrdered(10, lambda i: -i[1])
        print('recommend_title: top 10 by -i[i] {}'.format(recommend_title)) # 按相似度反序排列



if __name__ == '__main__':

    r = Recommend_cos()
    r.split_tab_require()
    rating = r.mlib_als()
    model = r.model(rating)

    movieByUser = r.sort_movieForUser(rating)
    print('movie By User 789 rating is  ', movieByUser[0].rating)
    predict_value = model.predict(789, movieByUser[0].product)
    print('movie By User 789 predict rating is ', predict_value)

    # 计算平方误差
    import math
    squaredError = math.pow(predict_value - movieByUser[0].rating, 2.0)
    print('计算的平方误差是: ', squaredError)

    # 对rating 的各个数据进行预测
    usersProducts = rating.map(lambda i: (i[0], i[1]))
    predictions = model.predictAll(usersProducts).map(lambda i: ((i.user, i.product), i.rating))
    print('predictions top 1 {}'.format(predictions.take(1)))

    # 重点 RDD 键值对 使用join拼 接两个 RDD
    ratingsAndPredictions = rating.map(lambda i: ((i.user, i.product), i.rating)).join(predictions)
    print('ratingsAndPredictons: ', ratingsAndPredictions.take(10))

    # 重点 RDD 误差
    mse = ratingsAndPredictions.map(lambda i: math.pow((i[1][0] - i[1][1]), 2)).reduce(add) / ratingsAndPredictions.count()
    print('mse: {}'.format(mse))

    # k值准确率
    def avgPrecisionK(actual, predicted, k):
        predK = predicted.take(k)
        score = 0
        numHits = 0
        for p, i in predK.zipWithIndex():
            if p in actual:
                numHits +=1
            score += numHits / (i + 1.0)

        if actual.isEmpty:
            return 1.0
        else:
            return score / min(len(actual), k)

    the_apk = map(lambda i:i.product, movieByUser)
    print('789 apk {}'.format(the_apk))


    #使用自带库进行评估
    from pyspark.mllib.evaluation import RegressionMetrics
    predictAndTrue = ratingsAndPredictions.map(lambda i: (i[1][0], i[1][1]))
    regressMetrics = RegressionMetrics(predictAndTrue)
    print('绝对误差 {}'.format(regressMetrics.meanAbsoluteError)) # MSE
    print('均方误差 {}'.format(regressMetrics.meanSquaredError)) # RMSE
    

