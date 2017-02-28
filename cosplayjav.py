#coding=utf-8

from lxml import etree
from peewee import SqliteDatabase, CharField, DateTimeField, IntegerField, Model

import os
import requests
import shutil
import zipfile

# database ============================================================================================================
db = SqliteDatabase(r'E:\cosplayjav.db3')


# Tool Methods=========================================================================================================
def gen_headers(referer=''):
    """;
    生成一个随机的请求头部

    :param referer: 请求头部的referer信息
    :return: 生成的请求头
    """
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 ' + \
                 'Safari/537.36'
    headers = {'User-Agent': user_agent, 'Accept-Language': 'zh-CN,zh;q=0.8', 'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Connection': 'keep-alive'}
    return headers


def save_img(src, name, dst):
    """
    将图片保存

    :param src: 图片地址
    :param name 图片名称
    :param dst 保存目录
    :return: 图片保存地址
    """
    # parse.quute会将':'也进行编码，因此需要手动处理引号
    suffix = src.split(".")[-1]
    file_name = os.path.join(dst, name + '.' + suffix)
    if os.path.exists(file_name):  # 已经下载过的图片
        return file_name
    response = requests.get(src, headers=gen_headers(), timeout=120)
    with open(file_name, 'wb') as _file:
        _file.write(response.content)
    return file_name


# cosplay data ========================================================================================================
class CosplayJavModel(Model):
    id = IntegerField(primary_key=True)
    url = CharField(max_length=1024)
    title = CharField(max_length=1024)
    img = CharField(max_length=1024)
    thumb = CharField(max_length=1024)
    status = IntegerField(default=0, index=True)

    class Meta:
        database = db

    @staticmethod
    def create_or_update(**kwargs):
        gm = CosplayJavModel.create_or_get(**kwargs)[0]
        changed = False
        for key in kwargs:
            if getattr(gm, key) is not kwargs[key]:
                setattr(gm, key, kwargs[key])
                changed = True
        if changed:
            res = gm.save()
            if res is not 1:
                raise Exception('Not updated! kwargs:', str(kwargs))


class MegaModel(Model):
    code = IntegerField(index=True)
    url = CharField(max_length=1024)
    mtype = IntegerField(default=1)

    class Meta:
        database = db

    @staticmethod
    def create_or_update(**kwargs):
        gm = MegaModel.create_or_get(**kwargs)[0]
        changed = False
        for key in kwargs:
            if getattr(gm, key) is not kwargs[key]:
                setattr(gm, key, kwargs[key])
                changed = True
        if changed:
            res = gm.save()
            if res is not 1:
                raise Exception('Not updated! kwargs:', str(kwargs))


# download object =====================================================================================================
class CosplayJav:
    """
    对应网站上一篇文章的实体类
    """

    def __init__(self):
        self.url = ''    # 文章地址
        self.code = ''   # 编号
        self.title = ''  # 标题
        self.img = ''    # 缩略图地址
        self.thumb = ''  # 详细图地址
        self.megas = []  # mega下载地址

    def create(self, code):
        """
        依据给出的code生成cos的所有信息
        :param code: cos的code
        :return: 生成的cos信息
        """
        self.code = code
        self.url = 'http://cosplayjav.pl/' + str(code)
        _content = requests.get(self.url, headers=gen_headers(), timeout=120).text
        tree = etree.HTML(_content)
        self.title = tree.xpath(u"/html/body/div[@class='container']/div[@class='row']//h1")[0].text
        self._gen_img_thumb(tree)
        self._gen_mega(tree)

    def _gen_img_thumb(self, tree):
        self.img = tree.xpath(u"/html/body/div[@class='container']//div[@class='post-thumb']/img/@src")[0]
        thumb_urls = tree.xpath(u"/html/body/div[@class='container']//div[@class='post-thumbnails']/a/@href")
        thumb_url = thumb_urls[0] if thumb_urls else None
        if thumb_url:
            _content = requests.get(thumb_url, headers=gen_headers(), timeout=120).text
            thumb_tree = etree.HTML(_content)
            self.thumb = thumb_tree.xpath(u"/html/body//img[@class='hidden img-thumbnails img-thumbnails-1']/@src")[0]
        else:
            self.thumb = ''

    def _gen_mega(self, tree):
        megas = tree.xpath(u"/html/body//div[@class='item-parts']/a/@href")
        for mega in megas:
            _content = requests.get(mega, headers=gen_headers(), timeout=120).text
            mega_tree = etree.HTML(_content)
            mega_url = mega_tree.xpath(u"/html/body//a[@class='btn btn-primary btn-download']/@href")[0]
            self.megas.append((mega_url, 0 if 'alternative' in mega else 1))

    def save_img(self, dst):
        save_img(self.img, str(self.code), dst)
        if self.thumb:
            save_img(self.thumb, str(self.code) + '_thumb', dst)

    def save_cos(self):
        db.connect()
        CosplayJavModel.create_or_update(id=self.code, url=self.url, title=self.title, img=self.img, thumb=self.thumb)
        for mega in self.megas:
            MegaModel.create_or_update(code=self.code, url=mega[0], mtype=mega[1])
        db.close()


# spider method =======================================================================================================
def get_codes_from_page(page):
    url = 'http://cosplayjav.pl/page/' + str(page) + '/'
    tree = etree.HTML(requests.get(url, headers=gen_headers(), timeout=120).text)
    codes = tree.xpath(u"/html/body//section[@id='main-section']/article/@id")
    codes = [int(code.split('-')[1]) for code in codes]
    return codes


def get_cos_from_list(codes, img_dic):
    fails = []
    for code in codes:
        try:
            cos = CosplayJav()
            cos.create(code)
            cos.save_img(img_dic)
            cos.save_cos()
            print('{:0>6} DONE: {}'.format(cos.code, cos.title))
        except Exception as e:
            print('{:0>6} FAIL.'.format(cos.code))
            fails.append(code)
    return fails


def delete_unused_files(cos_dic):
    for root, dirs, files in os.walk(cos_dic):
        for file in files:
            if file.endswith('url') or file == 'cosplayjav.pl.jpg' or file == 'cosplayjav.jpg':
                os.remove(os.path.join(root, file))


def zip_imgs(cos_dic):
    for root, dirs, files in os.walk(cos_dic):
        for d in dirs:
            if d == 'IMG':  # 找到了文件
                print('-----------------------------------------------------------------------------')
                print('[开始压缩]:', os.path.join(root, d))
                zfpath = os.path.join(root, d)
                with zipfile.ZipFile(zfpath + '.zip', mode='w', compression=zipfile.ZIP_STORED) as zf:
                    for r, ds, fs in os.walk(zfpath):
                        for file in fs:
                            src = os.path.join(r, file)
                            dst = src[len(root) + 1:]
                            zf.write(src, dst)
                print('[删除文件]:', os.path.join(root, d))
                shutil.rmtree(os.path.join(root, d))


if __name__ == '__main__':
    # delete_unused_files(r'E:\MEGA')
    # zip_imgs(r'E:\MEGA')

    cosDic = set()
    for cos in CosplayJavModel.select():
        cosDic.add(cos.id)

    root = r'E:\cos'
    page = 226

    while page < 314:
        ids = get_codes_from_page(page)
        download = []
        for id in ids:
            if id not in cosDic:
                download.append(id)
        if download:
            fails = get_cos_from_list(download, root)
            if fails:
                with open(r'E:\fails', 'a', encoding='utf-8') as file:
                    for fail in fails:
                        file.write(str(fail) + '\n')
        page += 1
        print('Page', page, 'Done ================================================================================')






