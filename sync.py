#!/usr/local/bin/py
import argparse
import hashlib
import logging
import os
import oss2  # pip install oss2
import re

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger('oss2').setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s] %(message)s',
    # filename='/tmp/oss-sync.log'
)

_CACHE = {}
ROOT_API_KEY = os.path.join(os.getenv('HOME'), '.aliyun')

# Doc: https://help.aliyun.com/knowledge_detail/5974206.htm
# 青岛节点外网地址： oss-cn-qingdao.aliyuncs.com
# 青岛节点内网地址： oss-cn-qingdao-internal.aliyuncs.com
#
# 北京节点外网地址：oss-cn-beijing.aliyuncs.com
# 北京节点内网地址：oss-cn-beijing-internal.aliyuncs.com
#
# 杭州节点外网地址： oss-cn-hangzhou.aliyuncs.com
# 杭州节点内网地址： oss-cn-hangzhou-internal.aliyuncs.com
#
# 上海节点外网地址： oss-cn-shanghai.aliyuncs.com
# 上海节点内网地址： oss-cn-shanghai-internal.aliyuncs.com
#
# 香港节点外网地址： oss-cn-hongkong.aliyuncs.com
# 香港节点内网地址： oss-cn-hongkong-internal.aliyuncs.com
#
# 深圳节点外网地址： oss-cn-shenzhen.aliyuncs.com
# 深圳节点内网地址： oss-cn-shenzhen-internal.aliyuncs.com
#
# 美国节点外网地址： oss-us-west-1.aliyuncs.com
# 美国节点内网地址：  oss-us-west-1-internal.aliyuncs.com
#
# 新加坡节点外网地址： oss-ap-southeast-1.aliyuncs.com
# 新加坡节点内网地址：  oss-ap-southeast-1-internal.aliyuncs.com
#
# 原地址oss.aliyuncs.com 默认指向杭州节点外网地址。
# 原内网地址oss-internal.aliyuncs.com 默认指向杭州节点内网地址
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


def sizeof_fmt(num):
    if num <= 1024:
        return '1 KB'
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def get_bucket(args):
    if 'bucket' in _CACHE:
        return _CACHE['bucket']

    api_key = open(os.path.join(ROOT_API_KEY, 'apikey')).read().strip()
    api_secret = open(os.path.join(ROOT_API_KEY, 'secretkey')).read().strip()
    auth = oss2.Auth(api_key, api_secret)
    bucket = oss2.Bucket(auth, API_URL, args.bucket)
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
                root = re.sub(r'^\./?', '', root)
                local_path = os.path.join(root, f)
                if is_in_ignore_files(local_path):
                    logging.info('ignored file: {}'.format(local_path))
                    continue
                md5 = get_file_md5(local_path)
                objects[local_path] = md5.upper()
                file_count += 1
    else:
        md5 = get_file_md5(oss_dir)
        local_path = re.sub(r'^\./', '', target_path)
        objects[local_path] = md5.upper()
        file_count += 1
    logging.info('local files: {}'.format(file_count))
    return objects


def get_remote_objects(args):
    objects = {'files': {}, 'etags': {}, 'meta': {}}
    bucket = get_bucket(args)
    marker = None
    file_count = 0
    prefix = re.sub(r'^\./?', '', args.target_path or '')
    while True:
        result = bucket.list_objects(prefix=prefix, max_keys=100, marker=marker)
        for obj in result.object_list:
            if obj.key.endswith('/'):
                continue
            if args.min_size and obj.size < args.min_size:
                continue
            if args.max_size and obj.size > args.max_size:
                continue
            if args.re and not re.search(args.re, obj.key):
                continue
            objects['files'][obj.key] = obj.etag
            objects['etags'][obj.etag] = obj.key
            objects['meta'][obj.key] = obj
            file_count += 1
        marker = result.next_marker
        if not result.is_truncated:
            break
    logging.info('remote files: {}'.format(file_count))
    return objects


def upload_file(local_path, args):
    bucket = get_bucket(args)
    key = re.sub(r'^\./?', '', local_path)
    res = bucket.put_object_from_file(key, local_path)
    if res.status != 200:
        logging.error('Upload {} failed. Exit.'.format(local_path))
        exit(1)


def upload_files_to_oss(args):
    target_path = re.sub(r'^\./?', '', args.target_path)
    logging.info('Uploading/Updating for: {}'.format(target_path))
    los = get_local_objects(target_path)
    if args.check_duplicated:
        ros = get_remote_objects(args)
    else:
        ros = get_remote_objects(args)

    files_need_to_update = []
    files_need_to_upload = []

    for local_path in los.keys():
        md5 = los[local_path]
        if md5 in ros['etags']:
            logging.info('* Identical file found:')
            logging.info('* @ {}'.format(ros['etags'][md5]))
            continue

        if local_path not in ros['files']:
            size = sizeof_fmt(os.path.getsize(local_path))
            files_need_to_upload.append((local_path, size))
        elif ros['files'][local_path] != md5:
            size = sizeof_fmt(os.path.getsize(local_path))
            files_need_to_update.append((local_path, size))

    files_need_to_update.sort()
    files_need_to_upload.sort()

    index = 1
    count = len(files_need_to_update)
    for local_path, size in files_need_to_update:
        if args.no:
            break
        elif args.yes:
            upload_file(local_path, args)
            index += 1
        else:
            print('Q: Do you want to update {}:'.format(local_path))
            response = input()
            while response.lower().strip() not in ('yes', 'no'):
                print('Q: Do you want to update {}:'.format(local_path))
                response = input()
            if response == 'no':
                logging.info('skipped {} by user'.format(local_path))
                continue
            logging.info('= [{}/{}] Updating old file: {} ({})'.format(
                index, count, local_path, size))
            upload_file(local_path, args)
            index += 1

    index = 1
    count = len(files_need_to_upload)
    for local_path, size in files_need_to_upload:
        try:
            logging.info('+ [{}/{}] Uploading new file: {} ({})'.format(
                index, count, local_path, size))
        except:
            pass
        upload_file(local_path, args)
        index += 1

    logging.info('Uploading/Updating Done\n')


def _get_dir_of_file(f):
    return '/'.join(f.split('/')[:-1])


def download_file(oss_path, local_path, args):
    dir_ = _get_dir_of_file(local_path)
    if not os.path.exists(dir_):
        os.makedirs(dir_)
    logging.info('+ Downloading {}'.format(oss_path))
    bucket = get_bucket(args)
    local_path = local_path.encode('utf-8')
    res = bucket.get_object_to_file(oss_path, local_path)
    if res.status != 200:
        logging.error('Download {} failed. Exit.'.format(oss_path))
        exit(1)


def list_files_on_oss(args):
    files = get_remote_objects(args)
    size_total = 0
    for o in files['meta']:
        size_total += files['meta'][o].size
        if args.verbose:
            print('\n- file: {}'.format(o))
            print('- size: {}'.format(sizeof_fmt(files['meta'][o].size)))
            print('- md5: {}'.format(files['meta'][o].etag))

    if not args.verbose:
        keys_to_list = list(files['files'].keys())
        keys_to_list.sort()
        print('== First 3 files:')
        for x in keys_to_list[:3]:
            print('   - {}'.format(x))
        print('== Last 3 files:')
        for x in keys_to_list[-3:]:
            print('   - {}'.format(x))

    print('\n== Total file count: {}'.format(len(files['files'])))
    print('== Total size: {}'.format(sizeof_fmt(size_total)))


def delete_files_from_oss(args):
    files = get_remote_objects(args)
    keys_to_delete = list(files['files'].keys())
    keys_to_delete.sort()
    print('== Will delete {} files:'.format(len(keys_to_delete)))
    print('== First 3 files:')
    for x in keys_to_delete[:3]:
        print('   - {}'.format(x))
    print('== Last 3 files:')
    for x in keys_to_delete[-3:]:
        print('   - {}'.format(x))

    answer = input('== Please enter YES to delete them ALL: ')
    if answer.strip() != 'YES':
        print('\nAction Canceled. Files are safe. Bye.')
        return

    bucket = get_bucket(args)
    count = 0
    for x in keys_to_delete:
        bucket.delete_object(x)
        count += 1
        print('- deleted: {}'.format(x))
    print('\nDeleted {} files.'.format(count))


def download_files_from_oss(args):
    target_path = args.target_path
    if target_path.startswith('./'):
        target_path = target_path[2:]
    if target_path.startswith('/'):
        raise ValueError('Must use relative path')

    oss_dir = os.path.dirname(__file__)
    oss_dir = os.path.join(oss_dir, '.')
    logging.info('Downloading file from: {}'.format(target_path))
    los = get_local_objects(target_path)
    ros = get_remote_objects(args)
    target_files = []
    for obj_key in ros['files']:
        if obj_key in los and ros['files'][obj_key] == los[obj_key]:
            logging.info('= {} exists'.format(obj_key))
            continue
        target_files.append(obj_key)

    target_files.sort()
    for oss_path in target_files:
        local_path = os.path.join(oss_dir, oss_path)
        download_file(oss_path, local_path, args)
    logging.info('Downloading Done\n')


def main():
    parser = argparse.ArgumentParser(description='Use Aliyun-OSS as Dropbox')
    parser.add_argument(
        '--target-path',
        '-p',
        action='store',
        const=None,
        default=None,
        help='Target path to sync/delete files'
    )
    parser.add_argument(
        '--download',
        '-d',
        action='store_true',
        default=False,
        help='Download files from OSS'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        default=False,
        help='overwrite existing files'
    )
    parser.add_argument(
        '--no',
        action='store_true',
        default=False,
        help='Do NOT overwrite existing files'
    )
    parser.add_argument(
        '--upload',
        '-u',
        action='store_true',
        default=False,
        help='Upload files to OSS'
    )
    parser.add_argument(
        '--listing',
        '-L',
        action='store_true',
        default=False,
        help='List files meta info on OSS'
    )
    parser.add_argument(
        '--min-size',
        type=int,
        default=0,
        help='[Listing] do not list size smaller than this'
    )
    parser.add_argument(
        '--max-size',
        type=int,
        default=0,
        help='[Listing] do not list size bigger than this'
    )
    parser.add_argument(
        '--re',
        type=str,
        default='',
        help='[Listing] filter file name by RE string'
    )
    parser.add_argument(
        '--check-duplicated',
        '-c',
        action='store_false',
        default=True,
        help='Do not upload files already in bucket other dirs'
    )
    parser.add_argument(
        '--bucket',
        '-b',
        required=True,
        help='bucket name to store data',
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='To delete files with prefix from OSS',
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Print more info',
    )

    args = parser.parse_args()
    if args.listing:
        list_files_on_oss(args)
    elif args.download:
        download_files_from_oss(args)
    elif args.delete:
        delete_files_from_oss(args)
    else:
        upload_files_to_oss(args)


if __name__ == "__main__":
    main()
