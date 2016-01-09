#!/usr/bin/python
import argparse
import codecs
import hashlib
import logging
import os
import re
import oss2

logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s] %(message)s',
    # filename='/tmp/oss-sync.log'
)

_CACHE = {}
BUCKET = 'vivid-db'
ROOT_API_KEY = os.path.join(os.getenv('HOME'), '.aliyun')
API_URL = 'oss-cn-hangzhou.aliyuncs.com'
IGNORE_FILES = (
    '\/\..*$',
    '\.pyc$',
)


def is_in_ignore_files(file_path):
    for p in IGNORE_FILES:
        if re.search(p, file_path):
            return True
    return False


def get_file_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()


def get_oss_path(local_path, oss_dir):
    path = local_path
    if oss_dir != '':
        path = path[len(oss_dir):]
    if not isinstance(path, str):
        path = path.decode('utf-8')
    path = re.sub('^[\./]*', '', path)
    return path


def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB','TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def is_dir(Key):
    return Key.ends_with('/')


def parse_files(content):
    files = {}
    content = xmltodict.parse(content)
    lbr = content['ListBucketResult']
    if not lbr.get('Contents'):
        return files, None, True

    if isinstance(lbr['Contents'], list):
        objects = lbr['Contents']
    else:
        objects = [lbr['Contents']]

    last_key = None
    for item in objects:
        k = item['Key']
        last_key = k
        v = item['ETag'].replace('"', '').lower()
        files[k] = v

    finished = lbr['IsTruncated'] == 'false'
    return files, last_key, finished


def get_bucket():
    if 'bucket' in _CACHE:
        return _CACHE['bucket']

    api_key = open(os.path.join(ROOT_API_KEY, '.apikey')).read().strip()
    api_secret = open(os.path.join(ROOT_API_KEY, '.secretkey')).read().strip()
    auth = oss2.Auth(api_key, api_secret)
    bucket = oss2.Bucket(auth, API_URL, BUCKET)
    _CACHE['bucket'] = bucket
    return bucket

def get_local_objects(target_path):
    objects = {}
    oss_dir = os.path.dirname(__file__)
    if target_path:
        oss_dir = os.path.join(oss_dir, target_path)
    else:
        oss_dir = os.path.join(oss_dir, '.')
    if not os.path.exists(oss_dir):
        return objects

    file_count = 0
    if os.path.isdir(oss_dir):
        for root, dirs, files in os.walk(oss_dir):
            for f in files:
                local_path = os.path.join(root, f)
                if is_in_ignore_files(local_path):
                    logging.info('ignored file: {}'.format(local_path))
                    continue
                md5 = get_file_md5(local_path)
                objects[local_path] = md5.upper()
                file_count += 1
    else:
        md5 = get_file_md5(oss_dir)
        objects[target_path] = md5.upper()
    logging.info('local files: {}'.format(file_count))
    return objects

def get_remote_objects(target_path):
    objects = {'files': {}, 'etags': {}}
    bucket = get_bucket()
    marker = None
    file_count = 0
    prefix = target_path or ''
    while True:
        result = bucket.list_objects(prefix=prefix, max_keys=100, marker=marker)
        for obj in result.object_list:
            if obj.key.endswith('/'):
                continue
            objects['files'][obj.key] = obj.etag
            objects['etags'][obj.etag] = obj.key
            file_count += 1
        marker = result.next_marker
        if not result.is_truncated:
            break
    logging.info('remote files: {}'.format(file_count))
    return objects


def upload_file(local_path, oss_path):
    conn = get_bucket()
    res = conn.put_object_from_file(BUCKET, oss_path, local_path)
    if res.status != 200:
        logging.error('Upload {} failed. Exit.'.format(local_path))
        exit(1)


def upload_files_to_oss():
    logging.info('Uploading/Updating Started')
    ros = get_remote_objects()
    ros_md5 = dict((v, k) for k, v in ros.items())
    logging.info('Total remote objects: {}'.format(len(ros)))

    los = get_local_objects()
    logging.info('Total local objects: {}'.format(len(los)))

    files_need_to_update = []
    files_need_to_upload = []

    for oss_path in los.keys():
        md5 = los[oss_path]['md5']
        local_path = los[oss_path]['local_path']
        if oss_path not in ros and md5 in ros_md5:
            logging.debug('* An Identical file to exists to {}'.format(oss_path))
            logging.debug('* - {}'.format(ros_md5[md5]))
            continue

        if oss_path not in ros:
            size = sizeof_fmt(os.path.getsize(local_path))
            files_need_to_upload.append((oss_path, local_path, size))
        elif ros[oss_path] != md5:
            size = sizeof_fmt(os.path.getsize(local_path))
            files_need_to_update.append((oss_path, local_path, size))

    files_need_to_update.sort()
    files_need_to_upload.sort()

    index = 1
    count = len(files_need_to_update)
    for oss_path, local_path, size in files_need_to_update:
        path_utf8 = oss_path.encode('utf-8')
        print('Do you want to update {}:'.format(path_utf8))
        response = raw_input()
        while response.lower().strip() not in ('yes', 'no'):
            print('Do you want to update {}:'.format(path_utf8))
            response = raw_input()
        if response == 'no':
            logging.info('skipped {} by user'.format(path_utf8))
            continue
        logging.info('= [{}/{}] Updating old file: {} ({})'.format(index, count, path_utf8, size))
        upload_file(local_path, oss_path)
        index += 1

    index = 1
    count = len(files_need_to_upload)
    for oss_path, local_path, size in files_need_to_upload:
        try:
            logging.info('+ [{}/{}] Uploading new file: {} ({})'.format(
                index, count, oss_path.encode('utf-8'), size))
        except:
            import pdb; pdb.set_trace()
            pass
        upload_file(local_path, oss_path)
        index += 1

    logging.info('Uploading/Updating Done\n')


def _get_dir_of_file(f):
    return '/'.join(f.split('/')[:-1])


def download_file(oss_path, local_path):
    dir_ = _get_dir_of_file(local_path)
    if not os.path.exists(dir_):
        os.makedirs(dir_)
    logging.info('+ Downloading {}'.format(oss_path))
    bucket = get_bucket()
    local_path = local_path.encode('utf-8')
    res = bucket.get_object_to_file(oss_path, local_path)
    if res.status != 200:
        logging.error('Download {} failed. Exit.'.format(oss_path))
        exit(1)


def download_files_from_oss(target_path):
    if target_path.startswith('/'):
        raise ValueError('Must use relative path')

    oss_dir = os.path.dirname(__file__)
    oss_dir = os.path.join(oss_dir, '.')
    logging.info('Downloading file from: {}'.format(target_path))
    los = get_local_objects(target_path)
    ros = get_remote_objects(target_path)
    target_files = []
    for obj_key in ros['files']:
        if obj_key in los and ros['files'][obj_key] == los[obj_key]:
            logging.info('= {} exists'.format(obj_key))
            continue
        target_files.append(obj_key)

    target_files.sort()
    for oss_path in target_files:
        local_path = os.path.join(oss_dir, oss_path)
        download_file(oss_path, local_path)
    logging.info('Downloading Done\n')


def sync():
    parser = argparse.ArgumentParser(description='Use Aliyun-OSS as Dropbox')
    parser.add_argument(
        '--target-path',
        '-p',
        action='store',
        const=None,
        default=None,
        help='Download files from OSS'
    )
    parser.add_argument(
        '--download',
        '-d',
        action='store_true',
        default=False,
        help='Download files from OSS'
    )
    parser.add_argument(
        '--upload',
        '-u',
        action='store',
        const=None,
        default=None,
        help='Upload files to OSS'
    )
    args = parser.parse_args()
    if args.download:
        download_files_from_oss(args.target_path or '')
    else:
        upload_files_to_oss()


if __name__ == "__main__":
    sync()
