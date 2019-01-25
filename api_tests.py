import threading
import  requests
import os
from urllib.parse import urljoin, quote, urlencode


EVHARBOR_AUTH_TOKEN = 'a99a6067059b5ff508c84d9694daa68f61f9af7c'
EVHARBOR_STORAGE_BUCKET_NAME = 'test'
# EVHARBOR_DOMAIN = 'obs.casearth.cn'
EVHARBOR_DOMAIN = '10.0.86.213:8000'
EVHARBOR_URL = 'http://%s' % EVHARBOR_DOMAIN

# 文件上传存储桶下的基目录
BASE_UPLOAD_DIR_NAME = '66/88'
# 文件下载保存位置，当前路径相对目录路径
BASE_DOWNLOAD_DIR_NAME = os.path.join('test_data', 'download')
# 要上传的文件所在的位置，当前路径相对目录路径
BASE_DOWNLOAD_FILES_DIR_NAME = os.path.join('test_data', 'upload')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 文件下载保存路径
BASE_DOWNLOAD_DIR = os.path.join(BASE_DIR, BASE_DOWNLOAD_DIR_NAME)

# 要上传的文件所在的路径
BASE_UPLOAD_FILES_DIR = os.path.join(BASE_DIR, BASE_DOWNLOAD_FILES_DIR_NAME)

# 文件上传基url
BASE_UPLOAD_URL = urljoin(EVHARBOR_URL, '/'.join(
    [item for item in ['api/v1/obj', EVHARBOR_STORAGE_BUCKET_NAME, BASE_UPLOAD_DIR_NAME] if item])) + '/'

API_V1_DIR_BASE_URL = urljoin(EVHARBOR_URL, 'api/v1/dir/')
API_V1_OBJ_BASE_URL = urljoin(EVHARBOR_URL, 'api/v1/obj/')


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
                        print(f'{self._filename},upload failed')
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

            print(f'{self._filename},upload successfull')

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
                    print(f'{self._obj_url},download ok')
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

        return [(o.get('na'), '/'.join([path, o.get('na')]).lstrip('/')) for o in objs_and_subdirs if o.get('fod')]

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
        bucket_name = bucket_name or self._bucket_name

        dirs = self.get_path_breadcrumb(dir_path, base_dir=base_dir)
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


def workers_generator(workers, num_per=10):
    '''
    包装生成器，每次从workers中取出num_per个元素

    :param workers: 生成器的源数据
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
    for fs in workers_generator(files, num_per=20):
        workers = []
        for filename in fs:
            worker = make_upload_worker(filename)
            worker.start()
            workers.append(worker)

        for w in workers:
           w.join()

def test_download(dir_path=''):
    '''
    下载存储桶根目录下的文件对象, 未做翻页下载
    '''
    direc = Directory()
    r = direc.get_objs_and_subdirs(dir_path=dir_path)
    if not r:
        print('获取存储桶下对象和目录列表信息失败')
        return

    obj_paths = direc.get_objs_path_list(r)
    workers = [make_download_worker(obj_path_name) for _, obj_path_name in obj_paths]

    for works in workers_generator(workers):
        for worker in works:
            worker.start()

        for worker in works:
            worker.join()

if __name__ == '__main__':

    # test_upload()
    test_download(dir_path=BASE_UPLOAD_DIR_NAME)






