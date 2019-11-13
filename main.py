import boto3
from botocore.configloader import load_config
import io
from sys import stdout


def format_bytes(bytes_num):
    sizes = ["B", "KB", "MB", "GB", "TB"]

    j = 0
    dblbyte = bytes_num

    while (j < len(sizes) and bytes_num >= 1024):
        dblbyte = bytes_num / 1024.0
        j = j + 1
        bytes_num = bytes_num / 1024

    return str(round(dblbyte, 2)) + " " + sizes[j]


def createBucket(credentials, configs):
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


if __name__ == '__main__':

    credentials = load_config("./.aws/credentials")
    configs = load_config("./.aws/configs")

    bucketFrom = createBucket(credentials['profiles']['from'], configs['profiles']['from'])
    bucketTo = createBucket(credentials['profiles']['to'], configs['profiles']['to'])

    i = 0
    total_size = 0
    for obj in bucketFrom.objects.all():
        if i>10:
            exit()
        key = obj.key
        objectFrom = bucketFrom.Object(key)

        file_stream = io.BytesIO()
        objectFrom.download_fileobj(file_stream)

        size = file_stream.getbuffer().nbytes
        total_size += size
        if size > 0:
            file_stream.seek(0)

            objectTo = bucketTo.Object(key)
            objectTo.upload_fileobj(file_stream)

        stdout.write("\rTotal files: %d, total size: %s" % (i, format_bytes(total_size)))
        stdout.flush()
        i += 1
