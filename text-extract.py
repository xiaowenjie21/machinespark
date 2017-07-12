
from pyspark import SparkContext
from machinespark import *

def create_vector(terms, term_dict):
    '''函数接受一个词列表，进行k之1编码，结果是scipy稀疏向量'''
    from scipy import sparse as sp
    num_terms = len(term_dict)
    x = sp.csc_matrix((1, num_terms))
    for t in terms:
        if t in term_dict:
            idx = term_dict[t]
            x[0, idx] = 1
    return x


def text_extract():
    '''从标题中提取标题，使用正则剔除非文本数据'''
    m = Movie_movie()
    movie_field = m.movie_fields
    raw_titles = movie_field.map(lambda fields: fields[1])
    movie_title = raw_titles.map(lambda i: extract_title(i))
    title_terms = movie_title.map(lambda i: i.split(' '))
    print('title_terms: ', title_terms.take(10)) # [['Toy', 'Story'], ['']]
    all_terms = title_terms.flatMap(lambda x: x).distinct().collect()
    print('all_terms: ', all_terms)

    idx = 0
    all_terms_dict = {}
    for all in all_terms:
        all_terms_dict[all] = idx
        idx+=1

    return title_terms, all_terms_dict


def extractTitle():
    '''使用其他方式获取数据'''
    m = Movie_movie()
    movide_filed = m.movie_fields
    movie_field = m.movie_fields
    raw_titles = movie_field.map(lambda fields: fields[1])
    movie_title = raw_titles.map(lambda i: extract_title(i))
    title_terms = movie_title.map(lambda i: i.split(' '))
    all_terms2 = title_terms.flatMap(lambda x: x).distinct().zipWithIndex().collectAsMap()
    print('flatmap: {}'.format(title_terms.flatMap(lambda x: x).distinct().take(5)))
    print('flatmap zip: {}'.format(title_terms.flatMap(lambda x: x).distinct().zipWithIndex().take(15)))
    return all_terms2

def convert_vector():
    title_terms , all_terms_dict = text_extract()
    all_terms_bcast = sc.broadcast(all_terms_dict)
    term_vectors = title_terms.map(lambda terms: create_vector(terms, all_terms_bcast.value))
    term_vectors.take(5)


def noralized():
    '''正则化数据'''
    np.random.seed(42)
    x = np.random.randn(10)
    norm_x_2 = np.linalg.norm(x)
    normalized_x = x / norm_x_2
    print("x:\n%s" % x)
    print("2-Norm of x: %2.4f" % norm_x_2)
    print("Normalized x:\n%s" % normalized_x)
    print("2-Norm of normalized_x: %2.4f" % np.linalg.norm(normalized_x))


if __name__ == '__main__':

    while 1:
        title_terms , all_terms_dict = text_extract()
        noralized()
        # dict_title = extractTitle()
        # print('dict_title: ', dict_title)
        # print('len dict:{}'.format(len(dict_title)))
        # print('dead len:{}'.format(dict_title['Dead']))
        # print('room {}'.format(dict_title['Rooms']))
