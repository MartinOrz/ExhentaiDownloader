#coding=utf-8

from lxml import etree
from peewee import SqliteDatabase, CharField, DateTimeField, IntegerField, Model

import os
import pickle
import re
import requests
import shutil
import time
import zipfile

# database ============================================================================================================
db = SqliteDatabase(r'E:\gmgard.db3')

# gm type defines =====================================================================================================
gm_types = {
    1: {'name': '文章', 'download': False, 'child': {10: '资讯', 11: '站务', 12: '心得感想', 13: '工具'}},
    2: {'name': '动画', 'download': True, 'child': {14: '商业动画', 15: '同人动画', 16: 'MMD', 17: '表番'}},
    3: {'name': 'CG', 'download': True, 'child': {18: '商业作CG', 19: '同人CG'}},
    4: {'name': '游戏', 'download': False, 'child': {20: '商业作', 21: '同人作', 22: '全年龄', 36: '补丁存档'}},
    5: {'name': '漫画', 'download': True, 'child': {23: '同人志', 24: '单行本', 25: '杂志', 26: '全年龄'}},
    6: {'name': '画集', 'download': True, 'child': {27: '工口画集', 28: '全年龄'}},
    7: {'name': '声乐', 'download': False, 'child': {29: '音乐', 30: '同人音声'}},
    8: {'name': '小说', 'download': False, 'child': {31: '官能小说', 32: '站友原创', 33: '一般小说'}},
    9: {'name': '绘画', 'download': False, 'child': {34: '一般绘画', 35: '工口绘画'}}
}


# gm data =============================================================================================================
class Gmgard(Model):
    id = IntegerField(primary_key=True)
    path = CharField()
    name = CharField()
    type1 = IntegerField()
    type2 = IntegerField(index=True)
    status = IntegerField(default=0, index=True)
    download_path = CharField()
    get_code = CharField()
    zip_password = CharField()
    img_url = CharField()
    upload_time = DateTimeField()

    class Meta:
        database = db

    @staticmethod
    def create_or_update(**kwargs):
        gm = Gmgard.create_or_get(**kwargs)[0]
        changed = False
        for key in kwargs:
            if getattr(gm, key) is not kwargs[key]:
                setattr(gm, key, kwargs[key])
                changed = True
        if changed:
            res = gm.save()
            if res is not 1:
                raise Exception('Not updated! kwargs:', str(kwargs))


# network tool methods ================================================================================================
def gen_headers():
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 ' + \
                 'Safari/537.36'
    h_cookie = ['__cfduid=d6e7867f69bfcdd8e8f0bf231e8247dbe1485955256',
                '.AspNetCore.Identity.Application=CfDJ8BKHE5DE5MNJrtBZVEo-oT_cnxDZQa74SRH3DmctY6KMD-qTb99E_Mqo8eI4AIvkXA-MH_0-a-wplAsTUcqaZkODhu8Aif3g4VT_Dd3ZOWq_TNKLVYEtmJ4CRG6NyKhdsOP9tGzzw_X-p2ZGeEZsQ_wkyasXhkreR09_Hi8tUEbfcawINSpw_V7eInRPncaNAxFeDeSLxLYCxH3OS_-hbCfHXMzAH9-Elxt5wI6SIvnSVhmHAB_H5vJ0ApLSgI5pDt0FMnUoeTpid2ceyFo6R2ph7Q77CyYzHs-2RnIpTVsJ_NyvkKEw8W5PtMzSA0Q5LTEQWZoC0Yv9MNIqEh4uu_XdXS7GivKBlkHCOHtfIA2YgpY3cA5EHvwCMnCIZMvHH4pQKSIILVKtHy3k4KzIxn_-nu6zBfWu9SOQ2l8oufAjffLT9t9P2UAmeNgYOEgYsKkkZ6qxBRl4ycjIPuBG1J90XgEDIVG8Vg7Ql73EaBnfplEXeYHXG1zKYCGfWFUIInnxp0T3ZSbkpaXp1RVdkI9VqnTYFEp6SA7o8tjrMKJ2']
    h_cookie = ';'.join(h_cookie)
    headers = {'User-Agent': user_agent, 'Accept-Language': 'zh-CN,zh;q=0.8', 'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Connection': 'keep-alive',
               'Cookie': h_cookie}
    return headers


# main page analysis tool methods =====================================================================================
def get_posts(tree):
    posts = tree.xpath(u"/html/body//div[@class='home-list']/div[@class='post ']/h3/a/@href")
    return [int(post.split('gm')[-1]) for post in posts]


def get_typed_posts(tree):
    posts = tree.xpath(u"/html/body//section/div[@id='listdiv']/ul/li/a/@href")
    return [int(post.split('gm')[-1]) for post in posts]


# detail page analysis tool methods ===================================================================================
pan_dl = re.compile(r'https{0,1}://pan\.baidu\.com/s/[\w\d]+')
pw = re.compile(r'密码.+?<span .+?>(.+?)</span>')
tm = re.compile(r'提取.+?<span .+?>(.+?)</span>')
post_time1 = re.compile(r'于 (\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+) ([AP]M) 发布')
post_time2 = re.compile(r'于 (\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+) 发布')


def get_gmpath(gmcode):
    return 'http://gmgard.com/gm{gmcode}'.format(gmcode=gmcode)


def get_title(tree):
    return tree.xpath(u"/html/body/div[@id='body']/div[@id='main']/div[@id='blog']/h2")[0].text.replace('\n', '')


def get_thumb_img(tree):
    imgs = tree.xpath(u"/html/body/div[@id='body']/div[@id='main']/div[@id='blog']/div[@id='imgdivs']/div/a/img/@src")
    if not imgs:
        return None
    dl = imgs[0]
    return 'http:' + dl if dl.startswith('//') else None


def get_type(tree):
    types = tree.xpath(u"/html/body/div[@id='body']/div[@id='main']/div[@id='blog']/a[@class='badge badge-info']/@href")
    if not types:
        return 0, 0
    if len(types) == 1:
        return int(types[0].split('/')[-1]), 0
    return int(types[0].split('/')[-1]), int(types[1].split('/')[-1])


def get_time(content):
    t = post_time1.findall(content)
    if t:
        month, day, year, hour, minute, second, t_type = t[0]
        hour = int(hour) if t_type == 'AM' else int(hour) + 12
    else:
        t = post_time2.findall(content)
        if not t:
            return "1970-01-01 00:00:00"
        year, month, day, hour, minute, second = t[0]
    return "{y}-{M:0>2}-{d:0>2} {H:0>2}:{m:0>2}:{s:0>2}".format(y=year, M=month, d=day, H=hour, m=minute, s=second)


def get_download_info(content):
    pan = pan_dl.findall(content)
    if not pan:
        return None
    get_code = tm.findall(content)
    if not get_code:
        get_code = ['']
    password = pw.findall(content)
    if not password:
        password = ['']
    return pan[0], get_code[0], password[0]


# log method ==========================================================================================================
def log(info_mode, *args):
    print(''.join(['[', info_mode, ']:']), end=' ')
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), end=' ')
    for arg in args:
        print(arg, end=' ')
    print('')


# analysis method =====================================================================================================
def get_gmpages(base, page):
    url = '?page='.join([base, str(page)])
    content = requests.get(url, headers=gen_headers()).text
    tree = etree.HTML(content)
    return get_posts(tree)


def get_typed_gmtypes(base, page):
    url = '?page='.join([base, str(page)])
    content = requests.get(url, headers=gen_headers()).text
    tree = etree.HTML(content)
    return get_typed_posts(tree)


def analysis_gmpage(gmcode, img_save_path):
    db.connect()
    result = True
    try:
        log('INFO', '==========================================================================================')
        log('INFO', '开始分析页面:', get_gmpath(gmcode))
        content = requests.get(get_gmpath(gmcode), headers=gen_headers()).text
        tree = etree.HTML(content)

        type1, type2 = get_type(tree)
        gmtype = gm_types[type1]
        log('INFO', '类型为:', gmtype['name'])
        if not gmtype['download']:
            log('INFO', '无需下载，结束.')
            return None

        title = get_title(tree)
        log('INFO', '名称:', title)

        download_info = get_download_info(content)
        utime = get_time(content)
        if not download_info:
            log('WARN', '没有下载信息，请检查页面.')
            # 强制指定 status 为3
            Gmgard.create_or_update(id=gmcode, path=get_gmpath(gmcode), name=title, type1=type1, type2=type2, status=3,
                                    download_path='', get_code='', zip_password='', img_url='', upload_time=utime)
            return
        pan, get_code, password = download_info
        img = get_thumb_img(tree)

        if img:
            img_type = img.split('.')[-1]
            img_content = requests.get(img, headers=gen_headers()).content
            with open(os.path.join(img_save_path, str(type2), '.'.join([str(gmcode), img_type])), 'wb') as file:
                file.write(img_content)

        # status 默认为0
        Gmgard.create_or_update(id=gmcode, path=get_gmpath(gmcode), name=title, type1=type1, type2=type2,
                                download_path=pan, get_code=get_code, zip_password=password, img_url=img,
                                upload_time=utime)
        log('INFO', '分析结束.')
    except Exception as e:
        log('ERROR', e)
        result = False
    db.close()
    return result


# zip method ==========================================================================================================
def _zip(root, filepath):
    log('INFO', '开始压缩', filepath)
    file_name = '.'.join([filepath, 'zip'])
    with zipfile.ZipFile(file_name, mode='w', compression=zipfile.ZIP_STORED) as zf:
        for r, dirs, files in os.walk(filepath):
            for file in files:
                src = os.path.join(r, file)  # 原文件地址
                dst = src[len(root) + 1:]  # 目标地址，去掉画集所在目录，例如文件为E:/test/gallery/1.jpg, 则得到gallery/1.jpg
                zf.write(src, dst)
    shutil.rmtree(filepath)


def zip_all(path):
    for d in os.listdir(path):
        zdir = os.path.join(path, d)
        if os.path.isdir(zdir):
            _zip(path, zdir)


if __name__ == '__main__':
    # codes = []
    # for page in range(30):
    #     log('INFO', '第', page + 100, '页分析结束.')
    #     codes += get_typed_gmtypes('http://gmgard.com/Blog/List/24', page + 100)
    # with open(r'e:\codes', 'wb') as file:
    #     pickle.dump(codes, file)
    # now = 1
    # errs = []
    # for code in codes:
    #     if not Gmgard.select().where(Gmgard.id == code):
    #         log('INFO', '第', now, '个任务开始.')
    #         now += 1
    #         time.sleep(1)
    #         if not analysis_gmpage(code, r'E:\GM'):
    #             errs.append(code)
    # with open(r'e:\errs', 'wb') as file:
    #     pickle.dump(errs, file)
    zip_all(r'e:\test1')