# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from QQMusicSpider.items import MusicItem
import json
from scrapy.exceptions import DropItem
import pymysql


class DuplicatesPipeline(object):
    """
    根据音乐的song_id，对爬取过的音乐进行去重
    """

    def __init__(self):
        self.song_ids = set()

    def process_item(self, item, spider):
        if isinstance(item, MusicItem):
            if item['song_id'] in self.song_ids:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.song_ids.add(item['song_id'])
                return item


class QqmusicspiderPipeline(object):
    def __init__(self):
        music_path = "music"
        self.file = open(music_path, "w", encoding="utf8")

    def process_item(self, item, spider):
        if isinstance(item, MusicItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.file.write(line)
        return item

    def close_spider(self, spider):
        self.file.close()


class MysqlPipeline(object):
    """
    同步操作
    """

    def __init__(self):
        # 建立连接
        self.conn = pymysql.connect(host='localhost', user='root', password='123456', db='qqmusic', port=3306,
                                    charset='utf8')  # 有中文要存入数据库的话要加charset='utf8'
        # 创建游标
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # sql语句
        insert_sql = """
        insert into qqmusic(album_name,language,lyric,singer_id,singer_mid,singer_name,song_id,song_mid,song_name,
        song_time_public,song_type,song_url,subtitle) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        # 执行插入数据到数据库操作
        self.cursor.execute(insert_sql, (item['album_name'], item['language'], item['lyric'], item['singer_id'],
                                         item['singer_mid'], item['singer_name'], item['song_id'], item['song_mid'],
                                         item['song_name'], item['song_time_public'], item['song_type'],
                                         item['song_url'],
                                         item['subtitle']))
        # 提交，不进行提交无法保存到数据库
        self.conn.commit()

    def close_spider(self, spider):
        # 关闭游标和连接
        self.cursor.close()
        self.conn.close()
