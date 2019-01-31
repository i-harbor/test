import threading
import  requests
import os
import argparse
import logging
from datetime import datetime
from urllib.parse import urljoin, quote, urlencode


log_filename = 'evharbor.log' # f'evharbor_{datetime.now().strftime("%Y-%m-%d %H-%M-%S")}.log'
logging.basicConfig(level=logging.WARNING, filename=log_filename, filemode='a')  # 'a'为追加模式,'w'为覆盖写
logger = logging.getLogger('log')

# EVHARBOR_DOMAIN = 'obs.casearth.cn'
EVHARBOR_DOMAIN = '10.0.86.213:8000'
EVHARBOR_URL = 'http://%s' % EVHARBOR_DOMAIN

API_V1_DIR_BASE_URL = urljoin(EVHARBOR_URL, 'api/v1/dir/')
API_V1_OBJ_BASE_URL = urljoin(EVHARBOR_URL, 'api/v1/obj/')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ObjUpload(threading.Thread):

    def __init__(self, objurl, base_dir, path, filename, *args, **kwargs):
        '''
        :param objurl: 对象url
        :param base_dir: 基目录路径
        :param path: 基目录路径base_dir下的相对路径
        :param filename: 要上传的文件绝对路径
        '''
        if not os.path.exists(filename):
            raise FileNotFoundError()

        self._filename = filename
        self._token = EVHARBOR_AUTH_TOKEN
        self._obj_url = objurl
        self._dir_path = path
        self._base_dir = base_dir
        super(ObjUpload, self).__init__(*args, **kwargs)

    def run(self):
        '''线程要执行的任务'''
        offset = 0
        with open(self._filename, 'rb') as f:
            for chunk in self.chunks(f):
                if not chunk:
                    break

                i = 0
                while True:
                    if i > 10:
                        t = f'[{self._filename}],upload failed'
                        logger.warning(t)
                        print(t)
                        return
                    try:
                        ok = self.upload_one_chunk(offset, chunk)
                        if ok:
                           break
                        elif ok is None:
                            i = 10

                    except requests.exceptions.ConnectionError as e:
                        pass
                    i += 1

                offset += len(chunk)

            print(f'[{self._filename}],upload successfull')

    def upload_one_chunk(self, offset, chunk):
        '''
        上传一个分片
        :param offset: 分片偏移量
        :param chunk: 分片
        :return:
            True: success
            False: failure
            None: 可能参数等各种原因不具备上传条件
        '''
        r = requests.put(self._obj_url, files={'chunk': chunk},
             data={"chunk_offset": offset, "chunk_size": len(chunk)}, headers={'Authorization': 'Token ' + self._token})

        if r.status_code == 200:
            return True
        elif r.status_code == 404: # 可能目录路径不存在
            d = Directory()
            if not d.create_path(dir_path=self._dir_path, base_dir=self._base_dir):
                return None
        elif 400 <= r.status_code < 500:
            return None

        return False

    def chunks(self, fd, chunk_size=5*1024**2):
        '''
        Read the file and yield chunks of ``chunk_size`` bytes

        :param fd: 文件描述符(file descriptor)
        :param chunk_size:
        :return:
        '''
        fd.seek(0)

        while True:
            data = fd.read(chunk_size)
            if not data:
                break
            yield data

    def size(self, fd):
        '''
        获取文件大小

        :param fd: 文件描述符(file descriptor)
        :return:
        '''
        if hasattr(fd, 'size'):
            return fd.size
        if hasattr(fd, 'name'):
            try:
                return os.path.getsize(fd.name)
            except (OSError, TypeError):
                pass
        if hasattr(fd, 'tell') and hasattr(fd, 'seek'):
            pos = fd.tell()
            fd.seek(0, os.SEEK_END)
            size = fd.tell()
            fd.seek(pos)
            return size
        raise AttributeError("Unable to determine the file's size.")


class ObjDownload(threading.Thread):

    def __init__(self, objurl, filename, *args, **kwargs):
        '''
        :param bucketname: 桶名
        :param objname:
        :param filename: 文件绝对路径
        '''
        self._filename = filename
        self._token = EVHARBOR_AUTH_TOKEN
        self._obj_url = objurl
        super(ObjDownload, self).__init__(*args, **kwargs)

    def run(self):
        offset = 0
        chunk_size = 5*1024*1024

        # 目录路径不存在存在则创建
        dir_path = os.path.dirname(self._filename)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        with open(self._filename, 'wb') as f:

            while True:
                ok, d = self.download_one_chunk(offset, chunk_size)
                if ok is None: # 文件不存在
                    print(f'{self._obj_url},文件不存在')
                    break
                elif not ok:
                    continue
                chunk = d.get('chunk', None)
                obj_size = d.get('obj_size', 0)

                f.seek(offset)
                f.write(chunk)

                offset += len(chunk)
                if offset >= obj_size: # 下载完成
                    print(f'[{self._obj_url}],download ok')
                    break

    def download_one_chunk(self, offset, size):
        '''
        上传一个分片
        :param offset: 分片偏移量
        :param size: 分片大小
        :return:
            True: success
            False: failure
        '''
        FAILTURE_RETURN = (False, None)
        GET_404_RETURN = (None, None)

        query_params = {'offset': offset, 'size': size}
        query_str = urlencode(query_params)
        url = self._obj_url + '?' + query_str

        try:
            r = requests.get(url, headers={'Authorization': 'Token ' + self._token})
        except Exception as e:
            return FAILTURE_RETURN

        if r.status_code == 200:
            chunk = r.content
            chunk_size = int(r.headers.get('evob_chunk_size', None))
            obj_size = int(r.headers.get('evob_obj_size', 0))

            if chunk_size is not None and chunk_size != len(chunk):
                return FAILTURE_RETURN

            return True, {  'chunk': chunk, 'obj_size': obj_size}

        elif r.status_code in [400, 404]:
            return GET_404_RETURN

        return FAILTURE_RETURN


class Directory():
    def __init__(self, bucket_name=None, dir_api_base_url=None):
        '''
        :param bucket_name: 目录操作对应的存储桶名称
        :param dir_api_base_url: 目录API的基url
        '''
        self._bucket_name = bucket_name or EVHARBOR_STORAGE_BUCKET_NAME
        self._dir_api_base = dir_api_base_url or API_V1_DIR_BASE_URL

    def create(self, dir_name, bucket_name=None, dir_path=''):
        '''
        创建一个目录

        :param bucket_name: 目录所在的存储桶名称
        :param dir_path: 目录的父目录节点路径
        :param dir_name: 要创建的目录
        :return:
            success: True
            failure: False
        '''
        bucket_name = bucket_name or self._bucket_name

        p = [bucket_name, dir_path, dir_name]
        if dir_path == '':
            p.pop(1)
        dir_path_name = '/'.join(p) + '/'
        dir_path_name = quote(dir_path_name)
        dir_url = urljoin(API_V1_DIR_BASE_URL, dir_path_name)

        while True:
            try:
                r = requests.post(dir_url, data={}, headers={'Authorization': 'Token ' + EVHARBOR_AUTH_TOKEN})
            except requests.exceptions.ConnectionError as e:
                pass
            else:
                break

        if r.status_code == 201:
            return True
        elif r.status_code == 400:
            data = r.json()
            if data.get('existing', '') is True:
                return True

        return False

    def get_objs(self, response_json):
        objs_and_subdirs = response_json.get('files', [])

        if not isinstance(objs_and_subdirs, list):
            return None
        return [o for o in objs_and_subdirs if o.get('fod')]

    def get_objs_path_list(self, response_json):
        objs_and_subdirs = response_json.get('files', [])
        path = response_json.get('dir_path')

        if not isinstance(objs_and_subdirs, list):
            return None

        return [(o.get('name'), '/'.join([path, o.get('name')]).lstrip('/')) for o in objs_and_subdirs if o.get('fod')]

    def get_objs_and_subdirs(self, bucket_name=None, dir_path='', params={}):
        '''
        获取目录下的对象和子目录

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param params: url query参数
        :return:
        '''
        dir_path_name = bucket_name or self._bucket_name
        if dir_path:
            dir_path_name += '/' + dir_path

        dir_path_name = quote(dir_path_name) + '/'

        query = urlencode(query=params)
        if query:
            dir_path_name += '?' + query

        dir_url = urljoin(API_V1_DIR_BASE_URL, dir_path_name)
        return self.get_objs_and_subdirs_by_url(dir_url)

    def get_objs_and_subdirs_by_url(self, dir_url):
        '''
        获取目录下的对象和子目录

        :param dir_url: 目录url
        :return:
        '''
        r = requests.get(dir_url, headers={'Authorization': 'Token ' + EVHARBOR_AUTH_TOKEN})
        if r.status_code == 200:
            return r.json()

        return None

    def create_path(self, bucket_name=None, dir_path='', base_dir=''):
        '''
        创建目录路径

        :param bucket_name: 目录所在的存储桶名称
        :param base_dir: 要创建的路径dir_path的基路经
        :param dir_path: 目录路径
        :return:
            success: True
            failure: False
        '''
        if dir_path == '':
            return True

        bucket_name = bucket_name or self._bucket_name

        dirs = self.get_path_breadcrumb(dir_path, base_dir=base_dir)
        # 先尝试创建路径最后一个目录，可能路径的前大半段已存在。如果成功不用从头尝试创建整个路径
        dir_name, p_dir_path = dirs[-1]
        if self.create(bucket_name=bucket_name, dir_path=p_dir_path, dir_name=dir_name):
            return True

        # 尝试从头创建整个路径
        for dir_name, p_dir_path in dirs:
            if not self.create(bucket_name=bucket_name, dir_path=p_dir_path, dir_name=dir_name):
                # 再次尝试
                if not self.create(bucket_name=bucket_name, dir_path=p_dir_path, dir_name=dir_name):
                    return False

        return True


    def get_path_breadcrumb(self, path=None, base_dir=''):
        '''
        路径面包屑
        :param base_dir: 基目录路径
        :return: list([dir_name，parent_dir_path])
        '''
        breadcrumb = []
        _path = path if isinstance(path, str) else ''
        if _path == '':
            return breadcrumb

        base = [base_dir] if base_dir else []
        _path = _path.strip('/')
        dirs = _path.split('/')
        for i, key in enumerate(dirs):
            breadcrumb.append([key, '/'.join(base + dirs[0:i])])
        return breadcrumb


def generator_wrapper(workers, num_per=10):
    '''
    包装生成器，每次从workers中取出num_per个元素

    :param workers: 生成器的源数据,需可被切片
    :param num_per: 每次返回的元素数量
    :return: workers的切片
    '''
    start = 0
    l = len(workers)
    while True:
        end = min(start + num_per, l)
        yield workers[start: end]

        if end == l:
            break
        start = end

def make_download_worker(obj_path_name):
    '''
    创建一个对象下载线程对象

    :param obj_path_name: 要下载的对象在存储桶下的全路径
    :return:
    '''
    obj_url = API_V1_OBJ_BASE_URL + EVHARBOR_STORAGE_BUCKET_NAME + '/' + obj_path_name + '/'
    save_file_name = os.path.join(BASE_DOWNLOAD_DIR, EVHARBOR_STORAGE_BUCKET_NAME, obj_path_name)
    return ObjDownload(objurl=obj_url, filename=save_file_name)

def make_upload_worker(filename):
    '''
    创建一个对象下载线程对象

    :param obj_path_name: 要下载的对象在存储桶下的全路径
    :return:
    '''
    obj_path_name = filename.replace(BASE_UPLOAD_FILES_DIR, '')
    obj_path_name = obj_path_name.lstrip('\\')
    obj_path_name = obj_path_name.replace('\\', '/')
    obj_url = BASE_UPLOAD_URL + obj_path_name + '/'
    return ObjUpload(objurl=obj_url, base_dir=BASE_UPLOAD_DIR_NAME, path=obj_path_name.rsplit('/', maxsplit=1)[0], filename=filename)

def get_upload_files(dir=None, max_get=10000):
    '''
    遍历目录下的所有文件，返回文件路径列表
    :param dir: 要遍历的目录
    :param max_get: 最多获取的文件数量, 此数值只是一个参考，判断的粒度是一个目录级的，最后返回的数量会大于此值
    :return:
    '''
    dir_path = dir or BASE_UPLOAD_FILES_DIR

    if not os.path.exists(dir_path):
        raise FileNotFoundError('要上传的文件目录路径不存在')

    files = []
    for base, dirs, filenames in os.walk(top=dir_path):
        for f in filenames:
            files.append(os.path.join(base, f))

        if len(files) > max_get:
            break

    return files

def test_upload():
    '''
    把指定路径下的文件上传到指定存储桶的路径下
    :return:
    '''
    direc = Directory()
    if not direc.create_path(dir_path=BASE_UPLOAD_DIR_NAME):
        raise Exception('创建存储桶内上传基目录路径失败')

    files = get_upload_files()
    for fs in generator_wrapper(files, num_per=20):
        workers = []
        for filename in fs:
            worker = make_upload_worker(filename)
            worker.start()
            workers.append(worker)

        for w in workers:
           w.join()

def test_download(dir_path=''):
    '''
    下载存储桶给定目录下的文件对象, 未做翻页下载
    '''
    direc = Directory()
    r = direc.get_objs_and_subdirs(dir_path=dir_path)
    if not r:
        print('获取存储桶下对象和目录列表信息失败')
        return

    obj_paths = direc.get_objs_path_list(r)
    workers = [make_download_worker(obj_path_name) for _, obj_path_name in obj_paths]

    for works in generator_wrapper(workers):
        for worker in works:
            worker.start()

        for worker in works:
            worker.join()

def get_args():
    parser = argparse.ArgumentParser(usage='''
    python api_tests.py -a upload -t a99a6067059b5ff508c84d9694daa6861f9af7c -b gggg -d uptest -p E:/work/test/test_data/download
    python api_tests.py --action=download --token=a99a6067059b5ff508c84d9694daa68f6f9af7c --bucket=gggg --dest=uptest --path=E:/work/test/test_data/download
    ''')
    parser.add_argument('-a', '--action',  # 参数名称, 选择上传或下载操作
        dest='action',              # 要添加到返回的对象的属性的名称
        nargs=1,                    # 当命令行有此参数时取值const, 否则取值default
        type=str,                   # 应转换命令行参数的类型。
        required=True,              # 参数选项必须
        choices=['upload', 'download'], # 参数允许值的容器
        help='Select upload or download to do',
        metavar='upload or download')    # 帮助信息中参数的示例值

    parser.add_argument('-b', '--bucket', # 参数名称
        dest='bucket_name',# dest - 要添加到返回的对象的属性的名称
        nargs=1,            # 参数值个数
        type=str,
        required=True,      # 参数选项必须
        help='bucket name', # help - 对参数的作用的简要说明。
        metavar='xxx'   # metavar - 帮助信息中参数的示例值
    )
    parser.add_argument('-t', '--token', # 参数名称
        dest='token',# 要添加到返回的对象的属性的名称
        nargs=1, # 参数值个数
        type=str,
        required=True,# 参数选项必须
        help='Auth token', # 对参数的作用的简要说明。
        metavar='xxx'    # 帮助信息中参数的示例值
    )
    parser.add_argument('-d', '--dest', #
        dest='dest',# 要添加到返回的对象的属性的名称
        nargs='?', # 输入了此参数未赋值时取值const, 未输入了此参数否则取值default
        const='',
        default='',
        type=str,
        required=False,# 参数选项不必须
        help='存储桶下的目录名，上传时表示文件上传到此目录下，下载时表示要下载此目录下文件，默认为存储桶下根目录',
        metavar='xx'   # 帮助信息中参数的示例值
    )
    parser.add_argument('-p', '--path',
        dest='path',# 要添加到返回的对象的属性的名称
        nargs='?', # 输入了此参数未赋值时取值const, 未输入了此参数否则取值default
        const='',
        default='',
        type=str,
        required=False,# 参数选项不必须
        help='本地路径，上传时表示要被上传的目录路径，下载时表示下载的文件要保存到此路径下，默认为当前路径',
        metavar='/home/test'    # 帮助信息中参数的示例值
    )
    args = parser.parse_args()
    return args

def pre_work():
    '''
    工作前的一些初始化工作
    :return:
        success: args, type:dict
        failed: None
    '''
    global EVHARBOR_STORAGE_BUCKET_NAME
    global EVHARBOR_AUTH_TOKEN
    global BASE_UPLOAD_DIR_NAME # 文件上传存储桶下的基目录
    global BASE_DOWNLOAD_DIR # 文件下载保存路径
    global BASE_UPLOAD_FILES_DIR # 要上传的文件所在的路径
    global BASE_UPLOAD_URL # 文件上传基url

    args = get_args()

    # bucket
    EVHARBOR_STORAGE_BUCKET_NAME = args.bucket_name[0]
    if not EVHARBOR_STORAGE_BUCKET_NAME:
        print('bucket name invalied')
        return None

    EVHARBOR_AUTH_TOKEN = args.token[0] # token
    BASE_UPLOAD_DIR_NAME = args.dest # bucket下目标的目录路径

    path = args.path
    if not path:
        BASE_DOWNLOAD_DIR = os.path.join(BASE_DIR, 'download') # 默认 保存下载文件的目录
        BASE_UPLOAD_FILES_DIR = BASE_DIR # 默认要被上传的路径
    else:
        if not os.path.exists(path):
            print(f'path "{path}" is not exists.')
            return None

        BASE_UPLOAD_FILES_DIR = BASE_DOWNLOAD_DIR = path # 要上传的目录 或 要保存下载文件的目录

    BASE_UPLOAD_URL = urljoin(EVHARBOR_URL, '/'.join(
        [item for item in ['api/v1/obj', EVHARBOR_STORAGE_BUCKET_NAME, BASE_UPLOAD_DIR_NAME] if item])) + '/'

    args.action = args.action[0]
    print_env_config(action=args.action)
    return args

def print_env_config(action):
    print(f'action: {action}')
    print(f'bucket name: {EVHARBOR_STORAGE_BUCKET_NAME}')
    print(f'token: {EVHARBOR_AUTH_TOKEN}')
    print(f"dest bucket's dir: {BASE_UPLOAD_DIR_NAME}")
    if action == 'upload':
        print(f'will be uploaded dir: {BASE_UPLOAD_FILES_DIR}')
    else:
        print(f'download save to: {BASE_DOWNLOAD_DIR}')


if __name__ == '__main__':
    args = pre_work()
    if not args:
        print('have some errors abort work.')
        os._exit(0)

    if args.action == 'upload':
        print('upload will be start')
        test_upload()
        print('upload done')
    else:
        print('download will be start')
        test_download(dir_path=BASE_UPLOAD_DIR_NAME)
        print('download done')







