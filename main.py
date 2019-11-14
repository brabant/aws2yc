import boto3
from botocore.configloader import load_config
import io
import asyncio
from sys import stdout
import argparse
import time


def format_bytes(bytes_num):
    sizes = ["B", "KB", "MB", "GB", "TB"]

    j = 0
    dblbyte = bytes_num

    while (j < len(sizes) and bytes_num >= 1024):
        dblbyte = bytes_num / 1024.0
        j = j + 1
        bytes_num = bytes_num / 1024

    return str(round(dblbyte, 2)) + " " + sizes[j]


def create_bucket(credentials, configs):
    params = {}
    for param, val in credentials.items():
        params[param] = val

    session = boto3.Session(**params)
    params = {}

    for param, val in configs.items():
        if param != 'bucket':
            params[param] = val

    resource = session.resource('s3', **params)

    return resource.Bucket(configs['bucket'])


def main_async(skip, max, only_new, threads_count):
    credentials = load_config("./.aws/credentials")
    configs = load_config("./.aws/configs")

    bucket_from = create_bucket(credentials['profiles']['from'], configs['profiles']['from'])
    bucket_to = create_bucket(credentials['profiles']['to'], configs['profiles']['to'])

    if skip:
        print("Skipping %d" % skip)
    keys = []

    i = 0
    total_size = 0

    print("Start at %d threads")

    for obj in bucket_from.objects.all():
        if i < skip:
            i += 1
            continue

        keys.append(obj.key)
        if len(keys) == threads_count:
            futures = []
            sizes = list(0 for _ in range(threads_count))
            for k in range(len(keys)):
                key = keys[k]

                async def copy_func(l=k, url=key):
                    object_to = bucket_to.Object(url)
                    if only_new:
                        objs = list(bucket_to.objects.filter(Prefix=url))
                        exists = True if (len(objs) > 0 and objs[0].key == key) else False
                    else:
                        exists = False
                    if not exists:
                        object_from = bucket_from.Object(url)

                        file_stream = io.BytesIO()
                        object_from.download_fileobj(file_stream)

                        file_size = file_stream.getbuffer().nbytes

                        if file_size > 0:
                            file_stream.seek(0)
                            object_to.upload_fileobj(file_stream)
                        sizes[l] = file_size

                futures.append(copy_func())
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(futures))

            for size in sizes:
                total_size += size
                i += 1
            keys = []

            stdout.write("\rTotal files: %d, total size: %s                           " % (i, format_bytes(total_size)))
            stdout.flush()

        if max != 0 and i > max:
            return

        i += 1


def main_sync(skip, max, only_new):
    credentials = load_config("./.aws/credentials")
    configs = load_config("./.aws/configs")

    bucket_from = create_bucket(credentials['profiles']['from'], configs['profiles']['from'])
    bucket_to = create_bucket(credentials['profiles']['to'], configs['profiles']['to'])

    if skip:
        print("Skipping %d" % skip)

    i = 0
    total_size = 0
    for obj in bucket_from.objects.all():
        stdout.write("\rTotal files: %d, total size: %s" % (i, format_bytes(total_size)))
        stdout.flush()

        if i < skip:
            i += 1
            continue

        key = obj.key

        object_to = bucket_to.Object(key)
        if only_new:
            objs = list(bucket_to.objects.filter(Prefix=key))
            exists = True if (len(objs) > 0 and objs[0].key == key) else False
        else:
            exists = False
        if not exists:

            object_from = bucket_from.Object(key)

            file_stream = io.BytesIO()
            object_from.download_fileobj(file_stream)

            file_size = file_stream.getbuffer().nbytes
            total_size += file_size
            if file_size > 0:
                file_stream.seek(0)
                object_to.upload_fileobj(file_stream)

        if max != 0 and i > max:
            return
        i += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Копирование файлов'
    )
    parser.add_argument('-s', '--skip', help='Пропустить первые s файлов', type=int, default=0)
    parser.add_argument('-m', '--max', help='Остановиться при достижении m файлов', type=int, default=0)
    parser.add_argument('-t', '--threads', help='Количество потоков', type=int, default=1)
    parser.add_argument('-n', '--onlynew', help='Только новые', type=int, default=1)
    args = parser.parse_args()
    skip = args.skip
    max = args.max
    only_new = args.onlynew
    threads_count = args.threads

    start_time = time.time()
    if threads_count > 1:
        main_async(skip, max, only_new, threads_count)
    else:
        main_sync(skip, max, only_new)
    print("")
    print("--- %s seconds ---" % (time.time() - start_time))
