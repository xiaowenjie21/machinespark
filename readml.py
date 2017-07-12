from pyspark import SparkContext
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import matplotlib
import numpy as np


sc = SparkContext('local[4]', 'app')


class Movie_user:

    def __init__(self):
        data = sc.textFile(r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.user')
        user_fields = data.map(lambda line: line.split('|'))
        self.user_fields = user_fields


    @property
    def num(self):
        num = self.user_fields.count()
        return num

    @property
    def num_user(self):
        # 统计用户数量
        num_user = self.user_fields.map(lambda i: i[0]).distinct().count()
        return num_user

    @property
    def num_genders(self):
        num_genders = self.user_fields.map(lambda i:i[2]).distinct().count()
        return num_genders

    @property
    def num_jobs(self):
        num_jobs = self.user_fields.map(lambda i: i[3]).distinct().count()
        return num_jobs


    @property
    def user_dict(self):
        user_dict = self.user_fields.map(lambda i: i[2]).countByValue()
        return user_dict

    @property
    def genders_dict(self):
        genders_dict = self.user_fields.map(lambda i:i[2]).countByValue()
        return genders_dict

    @property
    def jobs_dict(self):
        jobs_dict = self.user_fields.map(lambda i: i[3]).countByValue()
        return jobs_dict


    def __repr__(self):
        #统计用户数量
        #统计性别数据
        print('user filed: ', self.user_fields.take(10))
        print('总条数: ', self.num)
        print('各职业', self.num_jobs)
        print('职业以及占比', self.jobs_dict)
        return '....'

    def __call__(self, plt_type):
        if plt_type is 'hist':
            self.ages_hist()
        elif plt_type is 'bar':
            self.gener_bar()


    def ages_hist(self):

        user_fields = self.user_fields
        ages = user_fields.map(lambda x: int(x[1])).collect()
        plt.hist(ages, bins=20, color='lightblue', normed=True)
        fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(16, 10)
        plt.show()


    def gener_bar(self):
        user_fields = self.user_fields
        count_by_jobs = user_fields.map(lambda fields: (fields[3], 1)).reduceByKey(lambda x, y: x + y).collect()
        x_axis1 = np.array([c[0] for c in count_by_jobs])
        y_axis1 = np.array([c[1] for c in count_by_jobs])
        x_axis = x_axis1[np.argsort(-y_axis1)] #按升序 np.argssort(y_axis1)
        y_axis = y_axis1[np.argsort(-y_axis1)]


        pos = np.arange(len(x_axis))
        width = 1.0
        ax = plt.axes()
        ax.set_xticks(pos + (width / 2))
        ax.set_xticklabels(x_axis)
        plt.bar(pos, y_axis, width, color='lightblue')
        plt.xticks(rotation=30)
        fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(16, 10)
        plt.show()


    def get_occupation(self):
        user_fields = self.user_fields
        all_occupation = user_fields.map(lambda f: f[3]).distinct().collect()
        print('all_occupation: ', all_occupation)

        idx = 0
        all_occupations_dict = {}
        for o in all_occupation:
            all_occupations_dict[o] = idx
            idx += 1
            
        # 看一下“k之1”编码会对新的例子分配什么值
        print("Encoding of 'doctor': %d" % all_occupations_dict['doctor'])
        print("Encoding of 'programmer': %d" % all_occupations_dict['programmer'])

        k = len(all_occupations_dict)
        binary_x = np.zeros(k)
        k_programmer = all_occupations_dict['programmer']
        binary_x[k_programmer] = 1
        print('binary_x is: ', binary_x) #转换成k-1 的值
        print('k is: ', k)

        #对occupations的所有值进行转化
        for n, oc in enumerate(all_occupation):
            print('oc is: ', oc)
            k_value = all_occupations_dict[oc]
            binary_x = np.zeros(k)
            binary_x[k_value] = 1
            all_occupation[n] = binary_x

        print('all_occupation for n is: ', all_occupation)




class Movie_movie:
    def __init__(self):
        data = sc.textFile(r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.item')
        movie_fields = data.map(lambda line: line.split('|'))
        self.movie_fields = movie_fields



    def covert_year(self, x):
        '''转换年份'''
        try:
            return int(x[-4:])
        except:
            return 1990


    def count_year(self):
        '''统计电影年份分布'''

        def covert_year(x):
            '''转换年份'''
            try:
                return int(x[-4:])
            except:
                return 1990


        movie_fields = self.movie_fields
        years = movie_fields.map(lambda fields: fields[2]).map(lambda x: covert_year(x))
        years_filtered = years.filter(lambda x: x != 1900)
        movie_ages = years_filtered.map(lambda yr: 1998 - yr).countByValue()
        plt.hist(list(movie_ages.values()), bins=list(movie_ages.keys()), color='blue', normed=False)
        fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(16, 10)
        plt.show()


    def year_processed(self):

        def covert_year(x):
            '''转换年份'''
            try:
                return int(x[-4:])
            except:
                return 1990

        movie_fields = self.movie_fields
        year_pred_process = movie_fields.map(lambda field: field[2]).map(lambda x: covert_year(x)).collect()
        year_pred_process_array = np.array(year_pred_process)
        print('year_pred_process_array: ', year_pred_process_array)


        mean_year = np.mean(year_pred_process_array[year_pred_process_array != 1900])
        median_year = np.median(year_pred_process_array[year_pred_process_array != 1900])
        index_bad_data = np.where(year_pred_process_array == 1900)[0]
        year_pred_process_array[index_bad_data] = median_year
        print("Mean year of release: %d" % mean_year)
        print("Median year of release: %d" % median_year)
        print('index_bad_data is {}'.format(index_bad_data))



#---------define datetime-----------

def extract_datetime(ts):
    import datetime
    return datetime.datetime.fromtimestamp(ts)


def assign_tod(hr):
    times_of_day = {
    'morning' : range(7, 12),
    'lunch' : range(12, 14),
    'afternoon' : range(14, 18),
    'evening' : range(18, 23),
    'night' : [23, 24, 1, 2, 3, 4, 5, 6, 7]
    }
    for k, v in times_of_day.items():
        if hr in v:
            return k

#----------------------


#--------------textField filter -----------
def extract_title(raw):
    import re
    # 该表达式找寻括号之间的非单词（数字）
    grps = re.search("\((\w+)\)", raw)
    if grps:
    # 只选取标题部分，并删除末尾的空白字符
        return raw[:grps.start()].strip()
    else:
        return raw

#---------------

class Movie_score():
    '''电影评分数据'''
    def __init__(self, path = None):
        data = sc.textFile(r'C:\Users\qiniu\Downloads\Compressed\ml-100k\ml-100k\u.data')
        rating_score = data.map(lambda line: line.split('\t'))
        self.rating_score = rating_score


    @property
    def max_rating(self):
        max_ratings = self.rating_score.map(lambda i: int(i[2])).reduce(lambda x, y: max(x, y))
        return max_ratings

    @property
    def min_rating(self):
        min_ratings = self.rating_score.map(lambda i: int(i[2])).reduce(lambda x, y: min(x,y))
        return min_ratings

    @property
    def rating_dict(self):
        rating_dict = self.rating_score.map(lambda i: int(i[2])).countByValue()
        return rating_dict


    def __repr__(self):
        print('max_rating: ', self.max_rating, 'min_ratings: ', self.min_rating)
        return '....'



    def rating_data_info(self):
        '''显示第一个直方图, 评分占比'''
        #使用聚合函数找出最大值与最小值评分
        max_ratings = self.max_rating
        min_ratings = self.min_rating
        print('max_ratings: ', max_ratings, 'min_ratings: ', min_ratings)
        rating_hist = self.rating_dict
        values = rating_hist.values()
        keys = rating_hist.keys()

        print('values: ', values, 'keys: ', keys)

        plt.hist(list(values), color = 'blue', normed=True)
        fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(16, 9)
        plt.show()



    def ratings_bar(self):
        '''评分占比'''
        count_by_ratings = self.rating_dict
        x_axis = np.array([float(c) for c in count_by_ratings.keys()])
        y_axis = np.array([float(c) for c in count_by_ratings.values()])

        # 这里对y轴正则化，使它表示百分比
        y_axis_normed = y_axis / y_axis.sum()
        print('x_axis: ', x_axis, 'y_axis: ', y_axis)


        pos = np.arange(len(x_axis))
        width = 1.0
        ax = plt.axes()
        ax.set_xticks(pos + (width / 2))
        ax.set_xticklabels(x_axis)
        plt.bar(pos, y_axis_normed, width, color='lightblue')
        plt.xticks(rotation=30)
        fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(16, 10)

        plt.show() #从图中看出3.0分最多


    def user_ratings(self):
        '''每个用户评分的次数'''
        user_ratings_group = self.rating_score.map(lambda i: (int(i[0]), int(i[2]))).groupByKey()
        print('user_ratings_group: ', user_ratings_group.take(10))
        user_rating_byuser = user_ratings_group.map(lambda k_v: (k_v[0], len(k_v[1])))
        # user_rating_byuser = user_ratings_group.map(lambda (k, v): (k, len(v))) #此方法在python3中无法支持
        print(user_rating_byuser.take(5))
        #绘制用户评级分布图
        user_ratings_byuser_local = user_rating_byuser.map(lambda k_v: k_v[1]).collect()
        plt.hist(user_ratings_byuser_local, bins = 20, color = 'blue', normed=True)
        fig = plt.gcf()
        fig.set_size_inches(4, 3)
        plt.show()


    def time_format_hour(self):
        '''获取小时数, map的数据是str类型需要使用int进行转换'''
        timestamps = self.rating_score.map(lambda i : int(i[3]))
        hour_of_day = timestamps.map(lambda ts: extract_datetime(ts).hour)
        time_of_day = hour_of_day.map(lambda hr: assign_tod(hr))
        print(time_of_day.take(5))
        print('hour_of_day: ', hour_of_day.take(5))



if __name__ == '__main__':

    user = Movie_user()
    score = Movie_score()
    movie = Movie_movie()

    def run_user():
        print(user)
    
    def run_user_hist():
        user('hist')

    def run_user_bar():
        user('bar')


    def run_movie_count():
        movie.count_year()

    def run_score_info():
        print(score)

    def run_score_bar():
        score.ratings_bar()


    def run_year_prcessed():
        '''过滤异常数据'''
        movie.year_processed()



    # score.user_ratings()
    # run_year_prcessed()
    # user.get_occupation()
    score.time_format_hour()
    # print('user: ', user)
    # movie.count_year()
