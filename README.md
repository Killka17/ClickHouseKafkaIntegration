Отправляем целые числа в Kafka topic `numbers`, ClickHouse читает топик, невалидные сообщения отправляет в DLQ (`numbers_dlq`), а для валидных считает суммы положительных и отрицательных.

### Запуск

```bash
docker-compose up -d
```
Порты:
- Kafka: `localhost:9092`
- ClickHouse HTTP: `localhost:8123` (логин/пароль: `default` / `clickhouse`)
- ClickHouse native: `localhost:9001` (проброшен на `9000` внутри контейнера)

### Тест 

```bash
printf '10\n-7\n0\nabc\n  5 \n--1\n' | docker exec -i chi-kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic numbers
```

Ожидаемо:
- валидные: `10`, `-7`, `0`, `5`
- в DLQ уйдут: `abc`, `--1`

#### 2) Проверить суммы в ClickHouse

```bash
docker exec -i chi-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 \
  -u default --password clickhouse \
  --query "SELECT key, sum(pos_sum) AS pos_sum, sum(neg_sum) AS neg_sum FROM kafka_integration.sums GROUP BY key"
```

Для набора `10, -7, 0, 5` ожидаемо:
- `pos_sum = 15`
- `neg_sum = -7`
