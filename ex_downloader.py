#coding=utf-8

from enum import Enum, unique
from lxml import etree

import os
import PIL.Image as Image
import pickle
import queue
import re
import requests
import shutil
import time
import traceback
import threading
import zipfile

# globals
MEMBER_ID = '2631394'
PASS_HASH = '77ded0c31e820ed60d11e6ca9b458c72'
HAS_403 = False

ERR_IMG_PAGE = ''

LOG_ON = True


class NetworkDefs:

    timeout = 180  # 普通3分钟超时
    img_timeout = 300  # 图片5分钟超时


# XPath methods, gallery info =========================================================================================
def get_name_n(tree):
    """
    获取英文名称，英文名称一定存在（也可能是中文名称）
    :param tree: 需要解析的页面
    :return: 英文名称
    """
    return tree.xpath(u"/html/body/div[@class='gm']/div[@id='gd2']/h1[@id='gn']")[0].text


def get_name_j(tree):
    """
    获取日文名称，日文名称可能不存在
    :param tree: 需要解析的页面
    :return: 日文名称，不存在则为空字串
    """
    name_js = tree.xpath(u"/html/body/div[@class='gm']/div[@id='gd2']/h1[@id='gj']")
    return name_js[0].text if name_js else ''


def get_type(tree):
    """
    获取画集类型，获取结果为字符串
    :param tree: 需要解析的页面
    :return: 画集类型，字符串型结果
    """
    return tree.xpath(u"/html/body/div[@class='gm']/div[@id='gmid']/div[@id='gd3']/div[@id='gdc']/a/img/@alt")[0]


def get_basic_infos(tree):
    """
    获取画集的基本信息，包括语言，长度，发布时间，父画集
    :param tree: 需要解析的页面
    :return: dict类型结果，key为'Language', 'Length', 'Posted', 'Parent'
    """
    trs = tree.xpath(u"/html/body/div[@class='gm']/div[@id='gmid']/div[@id='gd3']/div[@id='gdd']//tr")
    result = dict()
    for tr in trs:
        tds = tr.xpath(u"td")
        key = tds[0].text[:-1]
        if key == 'Parent':
            # 如果是None, 则没有<a>标签，否则有<a>标签
            result[key] = tds[1].text if tds[1].text and tds[1].text == 'None' else tds[1].xpath(u"a")[0].text
        else:
            result[key] = tds[1].text
    return result


def get_tags(tree):
    """
    获取标签，包括作者，组，同人，角色，男性，女性，杂项
    :param tree: 需要解析的页面
    :return: dict类型结果，key为'artist', 'group', 'parody', 'character', 'male', 'female', 'misc'
    """
    trs = tree.xpath(u"/html/body/div[@class='gm']/div[@id='gmid']/div[@id='gd4']/div[@id='taglist']//tr")
    result = dict()
    for tr in trs:
        tds = tr.xpath(u"td")
        tag_type = tds[0].text[:-1]
        result[tag_type] = list()
        tags = tds[1].xpath(u"div/@id")
        for tag in tags:
            result[tag_type].append(tag.split(':')[-1])
    return result

# XPath methods, image info ===========================================================================================
ORI_IMG_INFO = re.compile(r'Download original (\d+) x (\d+) ([\d\.]+) (\w+) source')
IMG_INFO = re.compile(r'[^\.]+.(\S+) :: (\d+) x (\d+) :: ([\d\.]+) (\w+)')


def get_img_pages(tree):
    """
    获取图片的下载信息页，即从主页中解析出所有图片相信信息页地址
    :param tree: 需要解析的页面
    :return: 图像详细信息页面地址，为list格式
    """
    return tree.xpath(u"/html/body/div[@id='gdt']/div[@class='gdtm']//a/@href")


def get_ori_img(tree):
    nodes = tree.xpath(u"/html/body/div[@id='i1']/div[@id='i7']/a")
    if not nodes:
        return None
    node = nodes[0]
    o = dict()
    o['width'], o['height'], o['size'], o['size_type'] = ORI_IMG_INFO.findall(node.text)[0]
    o['source'] = node.get('href')
    return o


def get_img(tree):
    node = tree.xpath(u"/html/body/div[@id='i1']")[0]
    info = node.xpath(u"div[@id='i4']/div")[0]
    i = dict()
    i['source'] = node.xpath(u"div[@id='i3']/a/img/@src")[0]
    i['type'], i['width'], i['height'], i['size'], i['size_type'] = IMG_INFO.findall(info.text)[0]
    return i


def get_another_img(tree):
    src = tree.xpath(u"/html/body/div[@id='i1']/div[@id='i6']/a/@onclick")[0]
    return src[11:-2]  # return nl('15309-412281')


# general methods =====================================================================================================
def gen_headers():
    global MEMBER_ID, PASS_HASH
    if not MEMBER_ID:
        Exception('Please login first!')
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 ' + \
                 'Safari/537.36'
    h_cookie = 'nw=1;ipb_member_id=' + MEMBER_ID + ';ipb_pass_hash=' + PASS_HASH + ';'
    headers = {'User-Agent': user_agent, 'Accept-Language': 'zh-CN,zh;q=0.8', 'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Connection': 'keep-alive',
               'Cookie': h_cookie}
    return headers


def get_size(size, stype):
    size = float(size)
    if stype == 'B':
        return int(size)
    if stype == 'KB':
        return int(size * 1024)
    return int(size * 1024 * 1024)  # MB


def get_image_info(img):
    try:
        with Image.open(img) as file:
            width, height = file.size
            size = os.path.getsize(img)
            return width, height, size
    except Exception as e:
        log(LogLevel.ERROR, '获取图像信息失败:', e)
        return 0, 0, 0


def to_dict(obj):
    if isinstance(obj, list):  # 列表
        return [to_dict(ele) for ele in obj]
    if isinstance(obj, dict):  # 字典
        return {key: to_dict(obj[key]) for key in obj}
    if isinstance(obj, Enum):  # 枚举
        return obj.value
    if not hasattr(obj, '__dict__'):  # 其他内建类型
        return obj

    # 此时为自定义类，含有__dict__属性，对所有属性递归调用
    result = dict()
    for attr in obj.__dict__:
        if not attr.startswith('_'):
            value = obj.__dict__[attr]
            result[attr] = to_dict(value)
    return result


# log method ==========================================================================================================
@unique
class LogLevel(Enum):

    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4

LOG_PRINT_LEVEL = LogLevel.INFO


def log(info_mode, *args):
    global LOG_ON
    if info_mode.value >= LOG_PRINT_LEVEL.value and LOG_ON:
        print(''.join(['[', info_mode.name, ']:']), end=' ')
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), end=' ')
        for arg in args:
            print(arg, end=' ')
        print('')


@unique
class GalleryType(Enum):

    doujinshi = 1
    manga = 2
    artistcg = 3
    gamecg = 4
    western = 5
    nonh = 6
    imageset = 7
    cosplay = 8
    asianporn = 9
    misc = 10


@unique
class GalleryLanguage(Enum):

    Japanese = 1
    Chinese = 2
    English = 3
    Others = 4


class Gallery:

    def __init__(self, gallery_url):
        self.url = gallery_url
        self.id = 0
        self.name_n = None
        self.name_j = None

        self.type = GalleryType.doujinshi
        self.language = GalleryLanguage.Japanese
        self.is_translated = False

        self.length = 0
        self.posted = None
        self.parent = 0

        self.is_anthology = False
        self.parody = None
        self.character = None

        self.group = None
        self.artist = None

        self.male = None
        self.female = None
        self.misc = None

        self.img_info = dict()

    def gen_info(self):
        # 处理基本信息
        self.id = int(self.url.split('/')[4])
        log(LogLevel.INFO, '任务开始，地址:', self.url)

        # 获取名称以及类型
        _content = requests.get(self.url, headers=gen_headers(), timeout=NetworkDefs.timeout).text
        tree = etree.HTML(_content)
        self.name_n = get_name_n(tree)
        self.name_j = get_name_j(tree)

        # name_j可能不存在:
        if not self.name_j:
            self.name_j = self.name_n
        self.type = GalleryType[get_type(tree).replace('-', '')]
        log(LogLevel.INFO, '任务名称:', self.name_j)

        # 获取标签信息等，结果为字典类型
        basic_infos = get_basic_infos(tree)
        tags = get_tags(tree)

        # 长度等信息
        self.length = int(basic_infos['Length'].split(' ')[0])
        self.posted = basic_infos['Posted']
        if basic_infos['Parent'] != 'None':
            self.parent = int(basic_infos['Parent'])
        log(LogLevel.INFO, '任务长度:', self.length)

        # 语言信息，如果是中文
        language = basic_infos['Language'].split(' ')[0]
        self.language = GalleryLanguage.Others if language not in GalleryLanguage else GalleryLanguage[language]
        self.is_translated = 'language' in tags and 'translated' in tags['language']

        # 标签信息
        self.parody = tags['parody'] if 'parody' in tags else []
        self.character = tags['character'] if 'character' in tags else []
        self.is_anthology = 'misc' in tags and 'anthology' in tags['misc']
        self.group = tags['group'] if 'group' in tags else []
        self.artist = tags['artist'] if 'artist' in tags else []
        self.male = tags['male'] if 'male' in tags else []
        self.female = tags['female'] if 'female' in tags else []
        self.misc = tags['misc'] if 'misc' in tags else []

        imgs = list()
        now, page = 0, 0
        # 获取页面信息，第一页已经读取，直接使用当前tree即可
        while page < self.length:
            log(LogLevel.INFO, '第', str(now + 1), '页获取下载信息结束.')
            img_pages = get_img_pages(tree)
            imgs += img_pages
            page = len(imgs)
            if page < self.length:
                now += 1
                _content = requests.get(self.url + '?p=' + str(now), headers=gen_headers(),
                                        timeout=NetworkDefs.timeout).text
                tree = etree.HTML(_content)
        return imgs

    def get_dir_name(self):
        invalid = '/\\*|"'
        result = self.name_j
        for i in invalid:
            result = result.replace(i, ' ')
        result = result.replace('<', '《').replace('>', '》').replace('?', '？').replace(':', '：')
        result = ' '.join(['[exhentai]', '[{:0>8}]'.format(self.id), result])
        return result


class Dispatcher(threading.Thread):
    def __init__(self, gallery_url, worker_num, save_path):
        threading.Thread.__init__(self)
        self.name = 'Dispatcher'
        self.gallery = Gallery(gallery_url)
        self.queue = queue.Queue()
        self.worker_num = worker_num
        self.save_path = save_path
        self.done = False
        self.workers = list()

        self.check_right = False

    def stop(self):
        self.done = True

    def final_check(self):
        for page in self.gallery.img_info:
            img = self.gallery.img_info[page]
            dori = img.download_ori
            dic = img.ori if dori else img.src
            if 'type' not in dic:  # 如果下载成功一定有type
                return False
            file = os.path.join(self.save_path, ''.join(['{:0>3}'.format(page), '_ori.' if dori else '.', dic['type']]))
            if not os.path.exists(file):
                log(LogLevel.ERROR, '第', page, '页文件缺失')
                return False
            rw, rh, rs = get_image_info(file)
            if rw != dic['width']:
                log(LogLevel.ERROR, '第', page, '页Width 错误: {} : {}'.format(rw, dic['width']))
                return False
            if rh != dic['height']:
                log(LogLevel.ERROR, '第', page, '页Height 错误: {} : {}'.format(rh, dic['height']))
                return False
            if abs(dic['size'] - rs) >= (1024 * 10):
                log(LogLevel.ERROR, '第', page, '页Size 错误: {} : {}'.format(rs, dic['size']))
                return False
        return True

    def clean_dir(self):
        downloaded_pages = set()
        for page in self.gallery.img_info:
            img = self.gallery.img_info[page]
            dori = img.download_ori
            dic = img.ori if dori else img.src
            downloaded_pages.add(''.join(['{:0>3}'.format(page), '_ori.' if dori else '.', dic['type']]))
        downloaded_pages.add('gallery.pkl')
        for file in os.listdir(self.save_path):
            if file not in downloaded_pages:
                os.remove(os.path.join(self.save_path, file))

    def run(self):
        global HAS_403
        while True:
            try:
                imgs = self.gallery.gen_info()
                break
            except:
                log(LogLevel.WARN, '生成信息失败，重新开始生成信息.')
        self.save_path = os.path.join(self.save_path, self.gallery.get_dir_name())

        downloaded_imgs = dict()
        if os.path.exists(self.save_path):
            for file in os.listdir(self.save_path):
                name, suffix = file.split('.')
                if suffix != 'pkl':  # 图片文件
                    page = int(name.split('_')[0])
                    if name.endswith('ori') or (page not in downloaded_imgs):
                        downloaded_imgs[page] = os.path.join(self.save_path, file)
        else:
            os.mkdir(self.save_path)

        for i in range(len(imgs)):
            page = i + 1
            self.queue.put((page, None if page not in downloaded_imgs else downloaded_imgs[page], imgs[i]))

        for i in range(self.worker_num):
            worker = Worker(''.join(['worker', str(i)]), self.save_path, self.queue, self.gallery)
            self.workers.append(worker)
            worker.start()

        while not self.done:
            awake = 0
            for worker in self.workers:
                awake += 0 if worker.done else 1
            if self.queue.qsize() < 1 and awake == 0:
                self.stop()
            if HAS_403:
                log(LogLevel.WARN, '已经超过本日配额，停止下载.')
                self.stop()
        with open(os.path.join(self.save_path, 'gallery.pkl'), 'wb') as file:
            pickle.dump(to_dict(self.gallery), file)
        if self.final_check():
            log(LogLevel.INFO, '检查成功，下载完毕，删除多余文件.')
            self.clean_dir()
            self.check_right = True
        else:
            log(LogLevel.WARN, '检查失败，任务结束.')


class Worker(threading.Thread):
    def __init__(self, name, save_path, q, gallery):
        threading.Thread.__init__(self)
        self.name = name
        self.save_path = save_path
        self.done = False
        self.queue = q
        self.gallery = gallery

    def stop(self):
        self.done = True

    def run(self):
        global HAS_403
        while not self.done:
            taked = False
            downloaded = False
            try:
                page, downloaded_img, task = self.queue.get(timeout=5)
                taked = True
                if page not in self.gallery.img_info:
                    img_info = ImageDownloadTask(task, self.save_path, page, downloaded_img)
                    img_info.gen_image_info()
                    self.gallery.img_info[page] = img_info
                else:
                    img_info = self.gallery.img_info[page]
                if img_info.is_over_tried():
                    log(LogLevel.WARN, '图片', page, '下载次数过多，停止下载.')
                    continue
                download_err = img_info.download()
                if download_err is None:
                    log(LogLevel.INFO, '图片', page, '下载成功.')
                else:
                    log(LogLevel.ERROR, '图片', page, '下载失败: ', download_err)
                if HAS_403:
                    log(LogLevel.WARN, '本日下载已经超额，停止下载.')
                    self.stop()
            except queue.Empty:
                log(LogLevel.INFO, self.name, '下载完成，运行结束！')
                self.stop()
            except Exception as e:
                log(LogLevel.WARN, '下载出现问题, 已经拿到任务:', taked, '已经下载完成:', downloaded, '信息==>')
                traceback.format_exc()
                if taked and not downloaded:
                    self.queue.put(page, downloaded_img, task)


class ImageDownloadTask:

    def __init__(self, img_detail_url, save_path, page, downloaded_img):
        self.img_detail_url = img_detail_url
        self._save_path = save_path
        self.page = page
        self._downloaded_img = downloaded_img
        self._try_times = 0
        self.src = None
        self.ori = None
        self.download_ori = False
        self._now_download = ''
        self._next_try = None

    def gen_image_info(self):
        if self._try_times == 0:
            self._gen_image_info(self.img_detail_url)
        elif not self.download_ori:
            self._gen_image_info(self._next_try)

    def _gen_image_info(self, path):
        _content = requests.get(path, headers=gen_headers(), timeout=NetworkDefs.timeout).text
        tree = etree.HTML(_content)
        ori = get_ori_img(tree)
        if ori:
            self.ori = dict()
            self.ori['width'] = int(ori['width'])
            self.ori['height'] = int(ori['height'])
            self.ori['size'] = get_size(ori['size'], ori['size_type'])
            self.download_ori = True
            self._now_download = ori['source']
        src = get_img(tree)
        self.src = dict()
        self.src['width'] = int(src['width'])
        self.src['height'] = int(src['height'])
        self.src['size'] = get_size(src['size'], src['size_type'])
        self.src['type'] = src['type']
        if not self.download_ori:
            self._now_download = src['source']
            next_try = get_another_img(tree)
            if not self._next_try:
                self._next_try = ''.join([self.img_detail_url, '?nl=', next_try])
            else:
                self._next_try = ''.join([self._next_try, '&nl=', next_try])

    def get_file_name(self):
        dic = self.ori if self.download_ori else self.src
        return os.path.join(self._save_path, ''.join(['{:0>3}'.format(self.page), '_ori.' if self.download_ori else '.',
                                                     dic['type']]))

    def is_over_tried(self):
        return self._try_times > 3

    def check(self):
        dic = self.ori if self.download_ori else self.src
        if self._try_times == 0 and (self._downloaded_img is None):
            # 全新图片
            return 'Need download!'
        if self._downloaded_img is not None:
            # 已经下载过
            rw, rh, rs = get_image_info(self._downloaded_img)
            dic['type'] = self._downloaded_img.split('.')[-1]
        else:
            # 新一次下载完成
            name = self.get_file_name()
            rw, rh, rs = get_image_info(name)
        if rw != dic['width']:
            return 'Width 错误: {} : {}'.format(rw, dic['width'])
        if rh != dic['height']:
            return 'Height 错误: {} : {}'.format(rh, dic['height'])
        if abs(dic['size'] - rs) >= (1024 * 10):
            return 'Size 错误: {} : {}'.format(rs, dic['size'])
        return None

    def download(self):
        global HAS_403
        # 检查是否下载过原图
        result = self.check()
        if result is None:
            return None
        try:
            self._try_times += 1
            response = requests.get(self._now_download, headers=gen_headers(), timeout=NetworkDefs.img_timeout)
            _content = response.content
            suffix = response.url.split('?')[0].split('.')[-1]
            dic = self.ori if self.download_ori else self.src
            if suffix == 'php' or response.url == ERR_IMG_PAGE:  # 发生了403错误
                HAS_403 = True
                return '发生了403错误！'
            elif 'type' not in dic:
                dic['type'] = suffix
            name = self.get_file_name()
            with open(name, 'wb') as _file:
                _file.write(_content)
            return self.check()
        except Exception as e:
            return '下载错误: {}'.format(e)


def _re_download(ori_path, root_path):

    # 获取下载url
    if os.path.exists(os.path.join(ori_path, 'gallery.pkl')):
        with open(os.path.join(ori_path, 'gallery.pkl'), 'rb') as file:
            url = pickle.load(file)['root_path']
    elif os.path.exists(os.path.join(ori_path, 'gallery.dic')):
        with open(os.path.join(ori_path, 'gallery.dic'), 'rb') as file:
            url = pickle.load(file)['root_path']
    else:
        url = None
    if not url:
        return

    # 生成基本信息
    global LOG_ON
    LOG_ON = False
    try:
        gallery = Gallery(url)
        gallery.gen_info()
    except Exception as e:
        LOG_ON = True
        log(LogLevel.ERROR, '生成信息错误:', e)
        return

    # 拷贝所有文件到指定地址
    dst_path = os.path.join(root_path, gallery.get_dir_name())
    if dst_path != ori_path:
        if os.path.exists(dst_path):
            os.renames(ori_path, dst_path)
        else:
            shutil.rmtree(dst_path, ignore_errors=True)
            os.renames(ori_path, dst_path)

    LOG_ON = True
    d = Dispatcher(url, 3, root_path)
    d.start()
    while not d.done:
        time.sleep(2)


def re_download(src, dst):
    global HAS_403
    max_wait = 60 * 60 * 8
    now_wait = 60 * 30

    for dd in os.listdir(src):
        ori = os.path.join(src, dd)
        _re_download(ori, dst)
        if HAS_403:
            time.sleep(now_wait)
            HAS_403 = False
            if now_wait < max_wait:
                now_wait *= 2


def download(tasks, save_path):
    global HAS_403
    max_wait = 60 * 60 * 8
    now_wait = 60 * 30
    q = queue.Queue()
    for task in tasks:
        q.put(task)
    while True:
        try:
            task = q.get(timeout=5)
            d = Dispatcher(task, 1, save_path)
            d.start()
            while not d.done:
                time.sleep(2)
                if HAS_403:
                    break
            if not d.check_right:
                q.put(task)
            if HAS_403:
                time.sleep(now_wait)
                if now_wait < max_wait:
                    now_wait *= 2
                HAS_403 = False
        except queue.Empty:
            break


def _check_for_zip(path):
    log(LogLevel.INFO, '检查开始，路径:', path)
    pkl = os.path.join(path, 'gallery.pkl')
    if not os.path.exists(pkl):
        log(LogLevel.WARN, 'Gallery文件不存在，路径:', path)
        return False
    with open(pkl, 'rb') as file:
        gallery = pickle.load(file)
    if 'img_info' not in gallery:
        log(LogLevel.WARN, '非最新版本Gallery文件，路径:', path)
        return False
    if 1 not in gallery['img_info']:
        log(LogLevel.WARN, '非最新版本Gallery文件，路径:', path)
        return False
    for page in gallery['img_info']:
        img = gallery['img_info'][page]
        dori = img['download_ori']
        dic = img['ori'] if dori else img['src']
        if 'type' not in dic:  # 如果下载成功一定有type
            return False
        file = os.path.join(path, ''.join(['{:0>3}'.format(page), '_ori.' if dori else '.', dic['type']]))
        if not os.path.exists(file):
            log(LogLevel.ERROR, '第', page, '页文件缺失')
            return False
        rw, rh, rs = get_image_info(file)
        if rw != dic['width']:
            log(LogLevel.ERROR, '第', page, '页Width 错误: {} : {}'.format(rw, dic['width']))
            return False
        if rh != dic['height']:
            log(LogLevel.ERROR, '第', page, '页Height 错误: {} : {}'.format(rh, dic['height']))
            return False
        if abs(dic['size'] - rs) >= (1024 * 10):
            log(LogLevel.ERROR, '第', page, '页Size 错误: {} : {}'.format(rs, dic['size']))
            return False
    return True


def _zip(root, path):
    log(LogLevel.INFO, '=================================================================================')
    gallery_path = os.path.join(root, path)  # 画集顶级目录
    if not _check_for_zip(gallery_path):
        log(LogLevel.WARN, '检查失败，结束.')
        return
    log(LogLevel.INFO, '开始压缩，路径:', gallery_path)
    file_name = '.'.join([gallery_path, 'zip'])
    with zipfile.ZipFile(file_name, mode='w', compression=zipfile.ZIP_STORED) as zf:
        for r, dirs, files in os.walk(gallery_path):
            for file in files:
                src = os.path.join(r, file)  # 原文件地址
                dst = src[len(root) + 1:]  # 目标地址，去掉画集所在目录，例如文件为E:/test/gallery/1.jpg, 则得到gallery/1.jpg
                zf.write(src, dst)
    shutil.rmtree(gallery_path)


def zip_all(root):
    for d in os.listdir(root):
        if os.path.isdir(os.path.join(root, d)):
            _zip(root, d)


if __name__ == '__main__':
    re_download(r'E:\test\doujinshi', r'E:\done')
    # zip_all(r'E:\done')

    # tasks = ['https://exhentai.org/g/638555/0770ced1c5/']
    # download(tasks, r'E:\test')

    # r = r'E:\test1'
    # for roo, dirs, files in os.walk(r):
    #     for file in files:
    #         old = os.path.join(roo, file)
    #         new = os.path.join(r, file)
    #         os.renames(old, new)

    # r1 = r'E:\test1'
    # r2 = r'E:\test'
    # for d in os.listdir(r1):
    #     pkl = os.path.join(r1, d, 'gallery.pkl')
    #     with open(pkl, 'rb') as file:
    #         gtype = pickle.load(file)['type']
    #     os.renames(os.path.join(r1, d), os.path.join(r2, gtype, d))




