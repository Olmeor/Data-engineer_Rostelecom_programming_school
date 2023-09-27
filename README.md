# Data-engineer_Rostelecom_programming_school
## "Дата-инженер" Школа программирования Ростелеком
### Материалы курса

---

## Homework
В данной папке собраны мои решения домашних заданий за курс.  

---

## Certification
Аттестационная работа.

**Цель работы** - создание системы аналитики для обработки и визуализации данных использования услуги интерактивного телевидения.

**Описание** - разработка MVP системы, которая собирает, обрабатывает и анализирует данные по использованию услуги интерактивного телевидения, предоставляемой компанией Ростелеком. Система должна дать представление о поведении пользователей, популярности контента, частоте и длительности сессий
просмотра.

### Поставленные задачи

**Реализовать следующие пункты технического задания:**  
1. Сбор данных с использованием RT.Streaming:
Создание продюсера на Python, который будет симулировать данные о поведении пользователей интерактивного телевидения. Например:
    - ID пользователя
    - время начала и окончания просмотра
    - выбранный контент  
    и отправлять их в топик Kafka.  
2. Хранение сырых данных в RT.DataLake:  
Создание потребителя на Python для RT.Streaming, который будет читать данные и сохранять их в HDFS для долгосрочного хранения в формате CSV.
3. Обработка и агрегация данных с использованием Apache Hive в RT.DataLake:
Создание таблиц Hive для хранения данных из HDFS. Написание запросов для агрегации данных, таких как:
    - общее время просмотра по дням
    - популярность различного контента
    - активность пользователей по времени суток и т.д.
4. Перенос данных в GreenPlum (RT.Warehouse) и / или ClickHouse (RT.WideStore)  
Настройка процесса ETL на основании Apache Airflow продукта RT.Streaming, чтобы перенести обработанные данные из Hive в GreenPlum для сложных аналитических запросов.
5. Аналитика с использованием Python (Apache Zeppelin + Apache Spark = RT.DataLake)  
Использование библиотек Python для глубокого анализа данных, выявления инсайтов по данным, прогнозирования поведения пользователей
6. Визуализация данных с использованием Apache Superset (продукт RT.DataVision)
Создание интерактивных дашбордов на основе данных из GreenPlum и ClickHouse. Дашборды могут включать в себя графики такие как
    - активности пользователей
    - рейтинг просмотра каналов
    - гистограммы длительности просмотров

---

[Презентация](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/presentation.pptx) - презентация проекта powerpoint  
[pdf](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/presentation.pdf) - презентация проекта в формате pdf

### Генерация датасета

Генерация датасета на основе статистики кабельного канала https://www.powernet.com.ru/channels-stat  
HTML-страница преобразовывается в исходный CSV-файл

[python/make_tv_dataset_csv.py](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/python/make_tv_dataset_csv.py) - скрипт, генерирующий датасет на основании исходного файла. Датасет разделен на три таблицы согласно третьей нормальной формы. Генерация идет для 10000 пользователей в течении 7 предыдущих суток.

[dataset](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/tree/main/%D0%A1ertification/dataset) - папка с полученным датасетом (в т.ч. с будущим стримом из п. 2)

---

### 1. Сбор данных с использованием RT.Streaming

[python/tx_kafka_json.py](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/python/tx_kafka_json.py) - продюсер на Python, который симулирует данные о поведении пользователей интерактивного телевидения.

---

### 2. Хранение сырых данных в RT.DataLake

[python/rx_kafka_json_to_csv.py](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/python/rx_kafka_json_to_csv.py) - потребитель на Python для RT.Streaming, который будет читает данные и сохраняет их в HDFS

---

### 3. Обработка и агрегация данных с использованием Apache Hive в RT.DataLake

[sql/hive.sql](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/sql/hive.sql) - sql-скрипты для создание таблиц Hive для импорта данных из HDFS и запросов для агрегации данных.

---

### 4. Перенос данных в GreenPlum (RT.Warehouse)

[python/olejnikov_tv_dag.py](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/python/olejnikov_tv_dag.py) - ориентированный ациклический граф Airflow DAG для переноса обработанных данных из Hive в Greenplum.

[sql/greenplum.sql](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/sql/greenplum.sql) - sql-скрипты для аналитических запросов и создания материализованного представления для последующего использования в Superset. 

---

### 5. Аналитика с использованием Python (Apache Zeppelin + Apache Spark = RT.DataLake)

[python/olejnikov_tv_2JBN33XNC.zpln](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/python/olejnikov_tv_2JBN33XNC.zpln) - файл Apache Zeppelin на основе интерпретатора Spark с аналитическими запросами к таблицам Hive и диаграммами.

---

### 6. Визуализация данных с использованием Apache Superset продукт RT DataVision

[dashboard/Superset_dashboard.jpg](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/dashboard/Superset_dashboard.jpg) - результат визуализации данных.

[dashboard/dashboard_export](https://github.com/Olmeor/Data-engineer_Rostelecom_programming_school/blob/main/%D0%A1ertification/dashboard/dashboard_export_20230927T091411.zip) - экспортированные результаты визуализации.

