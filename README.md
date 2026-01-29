

Датасет для анализа:
https://www.kaggle.com/datasets/atulanandjha/temperature-readings-iot-devices

Цель:
1) Разбить данные для загрузки в PostgreSQL, используя схему снежинка.
2) Вычислить 5 самых жарких и самых холодных дней за год.
3) Отфильтровать in/out = in.
4) Поле noted_date перевести в формат ‘yyyy-MM-dd’ с типом данных date.
5) Очистить температуру по 5-му и 95-му процентилю.

Данные из датасета поступают в таком виде:

| id                                      | room_id/id   | noted_date         | temp | out/in |
|-----------------------------------------|--------------|--------------------|------|--------|
| __export__.temp_log_196134_bd201015     | Room Admin   | 08-12-2018 09:30   | 29   | In     |
| __export__.temp_log_196131_7bca51bc     | Room Admin   | 08-12-2018 09:30   | 29   | In     |
| __export__.temp_log_196127_522915e3     | Room Admin   | 08-12-2018 09:29   | 41   | Out    |
| __export__.temp_log_196128_be0919cf     | Room Admin   | 08-12-2018 09:29   | 41   | Out    |
| __export__.temp_log_196126_d30b72fb     | Room Admin   | 08-12-2018 09:29   | 31   | In     |
| __export__.temp_log_196125_b0fa0b41     | Room Admin   | 08-12-2018 09:29   | 31   | In     |
| __export__.temp_log_196121_01544d45     | Room Admin   | 08-12-2018 09:28   | 29   | In     |
| __export__.temp_log_196122_f8b80a9f     | Room Admin   | 08-12-2018 09:28   | 29   | In     |
| __export__.temp_log_196111_6b7a0848     | Room Admin   | 08-12-2018 09:26   | 29   | In     |
| __export__.temp_log_196112_e134aebd     | Room Admin   | 08-12-2018 09:26   | 29   | In     |
| __export__.temp_log_196108_4a983c7e     | Room Admin   | 08-12-2018 09:25   | 42   | Out    |
| __export__.temp_log_196108_4a983c7e     | Room Admin   | 08-12-2018 09:25   | 42   | Out    |
| __export__.temp_log_196101_d5ec7633     | Room Admin   | 08-12-2018 09:24   | 29   | In     |
| __export__.temp_log_196099_3b8ef67b     | Room Admin   | 08-12-2018 09:24   | 29   | In     |
| __export__.temp_log_196095_788b2c27     | Room Admin   | 08-12-2018 09:22   | 29   | In     |
| __export__.temp_log_196096_eafb59b6     | Room Admin   | 08-12-2018 09:22   | 29   | In     |

Используя Airflow, выгрузим наши данные на локальный компьютер и проведем первичный анализ.

После того как наши данные будут преобразованы, спроектируем схему звезда и переместим их в PostgreSQL.

 
# IoT Temperature Monitoring Schema

```mermaid
erDiagram
    DEVICE {
        int device_id PK
        string device_name
    }
    STATUS {
        int status_id PK
        string status_name
    }
    TEMPERATURE_READINGS {
        int reading_id PK
        int device_id FK
        int status_id FK
        timestamp noted_date
        float temperature
    }

    DEVICE ||--o| TEMPERATURE_READINGS : has
    STATUS ||--o| TEMPERATURE_READINGS : has

