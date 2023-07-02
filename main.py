# Отбор релевантных вакансий

# Импортируем библиотеки для работы с БД
import psycopg2
#Разбор данных в формате json
import json
# Работа с датой и временем
import datetime

# Устанавливаем соединение с БД
conn = psycopg2.connect(dbname='hr', user='app', password='12345678', host='192.168.2.144', port=6432)
#Создаем Курсор
cursor = conn.cursor()

# Открываем файл с новой вакансией
f = open("vacancy.json")

# Считываем из файла вакансию и разбираем ее
vacancy = json.load(f)

# Определяем источник вакансий: поле "canonical_url" должно содержать строку zarplata.ru"
if "canonical_url" not in vacancy or "zarplata.ru" not in vacancy["canonical_url"]:
    print("Не определяется источник вакансии")
    exit(-1)

# Есть ли у вакансии идентификатор?
if "id" not in vacancy:
    print("Нет идентификатора вакансим")
    exit(-1)
# При помощи запроса SQL извлекаем время обновления вакансии с одинаковым идентификатором в вакансии, если оно там есть
cursor.execute("select modified_time from hr_entity where src_id = '{0}'".format(vacancy["id"]))

# Если вакансия есть в БД, а в новой вакансии есть время изменения
if cursor.arraysize > 0 and "mod_date" in vacancy:

    # Извлекаем время обновления новой вакансии
    vacTime = datetime.datetime.strptime(vacancy["mod_date"].split("+")[0], "%Y-%m-%dT%H:%M:%S")

    # Извлекаем время обновления вакансии в БД
    record = cursor.fetchall()

    # Если время новой вакансии токое же или старше, то выходим
    if vacTime <= record[0][0]:
        print("Версия записи в БД актуальна")
        exit(0)

print("Запись в БД: " + vacancy)
