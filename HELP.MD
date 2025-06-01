
## Запуск

### 1. Соберите Flink-джобу (если используете Maven)

В папке flink-job выполните команду:
```bash
mvn clean package
````

`JAR`-файл в результате компиляции будет лежать в `flink-job/target/`

### 2. Запустите все сервисы

```bash
docker-compose up --build
```

### 3. Зайдите в Flink Web UI

[http://localhost:8081](http://localhost:8081)

### 4. Загрузите Flink-джобу вручную:

**Submit New Job** → \
**Add New** → \
выберите `flink-job-{version}-SNAPSHOT.jar` → \
**Submit** с параметром Entry class 
```java
com.hedgefo9.Main
```

---

## Остановка

```bash
docker-compose down
```

## Полная очистка с удалением томов

```bash
docker-compose down -v
```
