# Копирование файлов с Amazon.S3 в Yandex.Cloud

Скрипт на python3 копирует все файл из одного S3 совместимого хранилища в другое.
Например из Amazon.S3 в Yandex.Cloud
*Исходные файлы не удаляются!*

### Установка

Создайте файл .aws/credentials с таким содержимым:

```
[profile from]
region_name=us-east-1
aws_access_key_id = YOUR_ACCESS_KEY_FROM_STORAGE
aws_secret_access_key = YOUR_SECRET_KEY_FROM_STORAGE

[profile to]
aws_access_key_id = YOUR_ACCESS_KEY_TO_STORAGE
aws_secret_access_key = YOUR_SECRET_KEY_TO_STORAGE
```

Создайте файл .aws/config с таким содержимым:

```
[profile from]
bucket = YOU_BUCKET_FROM_STORAGE


[profile to]
endpoint_url = https://storage.yandexcloud.net
bucket = YOU_BUCKET_TO_STORAGE
```

Данные в .aws/credentials нужны для создания сессии, а в .aws/config для создания клиента и вы можете добавить туда любые необходимые параметры.

Далее неодходимо установить зависимости: 

```
pip install -r requirements.txt
```

### Пример запуска
```
python main.py
```


### Лицензия
MIT License

Copyright (c) 2019 brabant.ru
