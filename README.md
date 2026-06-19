# IoT Streaming Data Processing with Kafka, Flink, and PostgreSQL

Потоковая обработка данных от IoT-устройств с использованием Kafka, PostgreSQL и Flink (SQL/Table API).

## Архитектура

```
IoT Генератор → Kafka (iot-events) → Flink (Join + Windowing) → Kafka (iot-aggregates)
                                          ↓
                                    PostgreSQL (device_types lookup)
```

### Компоненты

- **IoT Event Generator**: Java приложение, генерирует события с датчиков (тип устройства, температура, влажность)
- **Kafka**: Message broker для потока событий
- **Flink**: Stream processing engine с SQL/Table API
  - Join с таблицей device_types из PostgreSQL
  - Оконная агрегация (TUMBLE 1 минута)
  - Вычисление: средняя температура, медиана влажности
- **PostgreSQL**: Справочник типов устройств и хранилище результатов

## Требования

- Docker
- Docker Compose
- Java 11+ (для локальной разработки)
- Maven 3.9+ (для локальной разработки)

## Быстрый старт

### 1. Клонирование/подготовка проекта

```bash
cd C:\Users\rufus\Desktop\BigData\Project
```

### 2. Сборка Docker образов

```bash
docker-compose build
```

### 3. Запуск всех сервисов

```bash
docker-compose up -d
```

Проверка статуса:
```bash
docker-compose ps
```

### 4. Инициализация PostgreSQL

База данных инициализируется автоматически через `postgres/init/ddl.sql` и `postgres/init/dml.sql` при первом запуске.

Проверка подключения к PostgreSQL:
```bash
docker exec -it project-postgres-1 psql -U iot_user -d iot_db -c "SELECT * FROM device_types;"
```

## Доступ к сервисам

- **Flink Web UI**: http://localhost:8081
- **Kafka**: localhost:9092
- **PostgreSQL**: localhost:5432
  - User: `iot_user`
  - Password: `iot_password`
  - Database: `iot_db`

## Развертывание Flink Job

### Вариант 1: Через SQL Client

```bash
docker exec -it project-flink-jobmanager-1 ./bin/sql-client.sh
```

Затем скопировать и выполнить содержимое `sql/flink-job.sql`.

### Вариант 2: Через Java приложение

Сборка JAR:
```bash
cd flink-job
mvn clean package
```

Копирование в Flink контейнер:
```bash
docker cp flink-job/target/iot-streaming-job.jar project-flink-jobmanager-1:/opt/flink/userjars/
```

Запуск job:
```bash
docker exec project-flink-jobmanager-1 ./bin/flink run -c com.iot.streaming.IoTStreamingJob /opt/flink/userjars/iot-streaming-job.jar
```

## Проверка потока данных

### 1. Проверка событий в Kafka (iot-events)

```bash
docker exec -it project-kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-events \
  --from-beginning \
  --max-messages 10
```

### 2. Проверка агрегированных результатов в Kafka (iot-aggregates)

```bash
docker exec -it project-kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-aggregates \
  --from-beginning \
  --max-messages 10
```

### 3. Запрос результатов из PostgreSQL

```bash
docker exec -it project-postgres-1 psql -U iot_user -d iot_db -c \
  "SELECT * FROM sensor_aggregates ORDER BY created_at DESC LIMIT 10;"
```

## Логирование

Просмотр логов сервисов:

```bash
# Все сервисы
docker-compose logs -f

# Конкретный сервис
docker-compose logs -f flink-jobmanager
docker-compose logs -f iot-generator
docker-compose logs -f postgres
```

## Остановка

```bash
docker-compose down

# С удалением томов данных
docker-compose down -v
```

## Структура проекта

```
.
├── docker-compose.yml          # Конфигурация всех сервисов
├── flink-conf.yaml             # Конфиг Flink
├── postgres/
│   └── init/
│       ├── ddl.sql             # DDL скрипты для PostgreSQL
│       └── dml.sql             # DML примеры и запросы
├── sql/
│   └── flink-job.sql           # SQL скрипты для Flink job
├── iot-generator/              # IoT генератор (Java)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/...
├── flink-job/                  # Flink приложение (Java)
│   ├── pom.xml
│   └── src/main/java/...
├── jars/                       # Папка для скомпилированных JAR файлов
└── README.md                   # Этот файл
```

## Особенности реализации

### Flink SQL/Table API

- **Source (Kafka)**: Читает события из топика `iot-events` в формате JSON
- **Lookup Table (PostgreSQL)**: Join с таблицей `device_types` для получения названий устройств
- **Windowing**: TUMBLE (Tumbling Window) 1 минута для агрегации
- **Aggregation**: 
  - `AVG(temperature)` — средняя температура
  - `PERCENTILE_CONT(0.5)` — медиана влажности
- **Sink (Kafka)**: Результаты записываются в топик `iot-aggregates`

### Переход между DataStream и Table API

В проекте используется `StreamTableEnvironment` для работы с обоими API:
- DataStream API для сложной логики обработки
- Table API/SQL для JOIN и агрегаций

## Troubleshooting

### Kafka не доступна из Flink
Убедитесь, что используется правильный адрес брокера: `kafka:29092` (внутри Docker сети), а не `localhost:9092`.

### PostgreSQL connection refused
Проверьте здоровье контейнера:
```bash
docker-compose ps postgres
```

Дождитесь, пока статус не станет `healthy`.

### Flink job fails to start
Проверьте логи:
```bash
docker-compose logs flink-jobmanager
```

Убедитесь, что все зависимости загружены в `/opt/flink/userjars/`.

## Лицензия

MIT
