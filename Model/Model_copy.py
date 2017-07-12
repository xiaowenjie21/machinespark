from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, Numeric, DateTime, Text
from sqlalchemy import select
import pytest


class Engine:
    '''Mysql engine'''

    def __init__(self):
        self.engine = create_engine("mysql+pymysql://qiniu:mypwd@139.159.212.35:3306/qiniudata?charset=utf8",
                               encoding='utf8', echo = True)
        self.metadata = MetaData()

    @property
    def T_content(self):
        content = Table('content', self.metadata,
                        Column('neirong', Text, nullable=False ),
                        Column('url', String(255), nullable=False, primary_key=True),
                        Column('title', String(255), nullable=False),
                        Column('date', DateTime, nullable=False),
                        Column('author', String(255), nullable=True),
                        Column('editorer', String(255)),
                        Column('laiyuan', String(255)),
                        Column('collnum', Integer),
                        Column('commentnum', Integer),
                        Column('sitename', Integer, nullable=False),
                        Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
                        Column('cover', String(255)))
        return content

    @property
    def T_wx(self):
        wx = Table('wx', self.metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   Column('title', String(255)),
                   Column('date', DateTime),
                   Column('user', String(255)),
                   Column('neirong', Text),
                   Column('fileid', String(255)),
                   Column('url', String(255), primary_key=True),
                   Column('content_title', String(255)),
                   Column('cover', String(255)),
                   Column('author', String(255)))
        return wx

    @property
    def T_history(self):
        history = Table('history', self.metadata,
                        Column('username', String(255), primary_key=True),
                        Column('website', String(255), primary_key=True),
                        Column('date', DateTime, nullable=False),
                        Column('add_num', Integer, nullable=False),
                        Column('flag', String(255)),
                        Column('websitename', String(255), nullable=False),
                        Column('end_time', DateTime, nullable=False),
                        Column('crawl_num', Integer),
                        Column('have_num', Integer))
        return history


    @property
    def T_videocache(self):
        videocache = Table('videocache', self.metadata,
                           Column('source', String(255), nullable=False),
                           Column('url', String(255), primary_key=True),
                           Column('id', Integer, primary_key=True),
                           Column('title', String(255), nullable=False),
                           Column('d_time', DateTime),
                           Column('downurl', String(255)),
                           Column('create_time', DateTime),
                           Column('video_source', String(255)),
                           Column('category', String(255))
                           )
        return videocache


    def create_engine(self):
        self.metadata.create_all(self.engine)


class Sql:
    def __init__(self):
        e = Engine()
        e.create_engine()
        self.e = e
        self.connection = e.engine.connect()

    def sql_insert(self, table, **kwargs):
        ins = table.insert().values(**kwargs)
        rp = self.connection.execute(ins)
        if rp:
            return 1
        else:
            return 0



if __name__ == '__main__':
    m = Sql()
    content = m.e.T_content
    m.sql_insert(content, url = '1111')

