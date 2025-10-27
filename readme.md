## О проекте
ETL-пайплайн для мониторинга друзей ВКонтакте с системой оповещений и аналитикой. Проект автоматически отслеживает изменения в списке друзей и предоставляет удобный дашборд для анализа социальных связей.
## Ключевые возможности
* 📈 __ETL-процесс:__ Извлечение данных с помощью VK API, трансформация и загрузка в PostgreSQL;
* 🔔 __Система оповещений:__ Уведомления в Telegram об удаливших друзьях и неактивных пользователях;
* 📊 __Визуализация:__ Интерактивный дашборд в Metabase для анализа портрета друзей пользователя + pgAdmin4 для более удобного взаимодействия с БД;
* 🐳 __Контейнеризация:__ Полная Docker-сборка для простого развертывания;
* ⚙️ __Автоматизация:__ Оркестрация процессов через Apache Airflow.
## Архитектура
```mermaid
graph TB
    A[VK API] --> B[Apache Airflow]
    
    subgraph B [Apache Airflow DAG]
        C[Извлечение данных]
        D[Трансформация]
        E[Валидация]
        H[Telegram Уведомления]
        
        C --> D
        D --> E
        D --> H        
    end
    
    E --> F[PostgreSQL]
    F --> G[Metabase Dashboard]
    I[pgAdmin4] --> F
```
## Быстрый старт
### Предварительные требования
* Docker & Docker Compose
* VK Access Token с правами offline, friends
* Telegram Bot Token и Chat ID
### Конфигурация
Для получения __VK Access Token__:
1. Перейдите на https://vk.com/dev
2. Создайте Standalone-приложение
3. Получите token с правами: friends, offline
###
__Telegram Бот__:
1. Создайте бота через @BotFather
2. Получите token бота
3. Узнайте ваш chat ID (отправьте сообщение боту и проверьте updates)
### Установка и запуск
1. Клонируйте репозиторий:\
```git clone https://github.com/witchyburn/vk_friends_analyzer.git```\
```cd vk_friends_etl_project```
2. Отредактируйте ```.env``` файл:
```
# Analytical DB
DATABASE_USER=your_username
DATABASE_PASSWORD=your_password
DATABASE_NAME=your_dbname

# Airflow DB
AIRFLOW_DB_NAME=your_airflow_dbname
AIRFLOW_DB_USER=your_airflow_username
AIRFLOW_DB_PASSWORD=your_airflow_password

# Metabase DB
METABASE_DB_NAME=your_metabase_dbname
METABASE_DB_USER=your_metabase_username
METABASE_DB_PASSWORD=your_metabase_password

# pgAdmin4 settings
PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=your_password

# Airflow settings
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://your_airflow_username:your_airflow_password@postgres/your_airflow_dbname
AIRFLOW__WEBSERVER__DEFAULT_USER_USERNAME=airflow
AIRFLOW__WEBSERVER__DEFAULT_USER_PASSWORD=airflow
AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC

# Other
VK_ACCESS_TOKEN=your_token_here
VK_USER_ID=123456789
LOG_LEVEL=INFO
TG_BOT_TOKEN=your_token_here
TG_CHANNEL_ID=-123456789
```
3. Сделайте исполняемым файл ```postgres/init-databases.sh```:\
```chmod +x postgres/init-databases.sh```
4. Запустите приложение:\
```docker compose up -d```
5. Откройте интерфейсы:
* Airflow: http://localhost:8080
* pgAdmin4: http://localhost:5000
* Metabase: http://localhost:3000 
6. В Airflow UI перейдите в раздел __Admin__ -> __Connections__ и отредактируйте подключение __postgres_default__ см. в разделе __Скриншоты проекта__;
7. В Airflow UI перейдите в раздел __Admin__ -> __Variables__ и внесите значения переменных __VK_ACCESS_TOKEN__, __VK_USER_ID__, __TG_BOT_TOKEN__, __TG_CHANNEL_ID__ (опционально, см. в разделе __Скриншоты проекта__);
8. После выполнения шагов 6 и 7 можно запустите даги и убедитесь, что они отработали корректно;
9. Авторизуйтесь в Metabase, подключитесь к БД;
10. Все готово для того, чтобы построить аналитический дашборд!
## 🔄 Планировщик
Airflow DAG'и настроены на ежедневное выполнение в 00:10 и 00:15
## Мониторинг и Оповещения
🔴 Удалившие друзья: Уведомления при обнаружении исходящих заявок\
🟡 Неактивные друзья: Оповещения о пользователях, давно не заходивших в VK\
🟢 Статистика: Ежедневные отчеты об изменениях
## 📊 Аналитический Дашборд
Metabase дашборд может включать:
* Динамика количества друзей
* Географическое распределение
* Активность друзей
* Демографическая статистика
* История изменений и др.
###
Пример дашборда см. в разделе __"Скриншоты проекта"__.
## 📈 Мониторинг и логи
* Airflow UI: Мониторинг выполнения DAG
* PostgreSQL: Логи изменений в БД
* Файловые логи: Подробные логи ETL процессов
## Скриншоты проекта
<div align="center">
  <strong>Редактирование соединения в Airflow UI</strong><br>
  <img src="assets/airflow_ui_conn.png" alt="Редактирование соединения в Airflow UI" width="70%">

  <strong>Добавление переменной в Airflow UI</strong><br>
  <img src="assets/airflow_ui_vars.png" alt="Добавление переменной в Airflow UI" width="70%">

  <strong>Лог задачи в Airflow UI</strong><br>
  <img src="assets/airflow_ui_logs.png" alt="Лог задачи в Airflow UI" width="70%">
    
  <strong>Пример дашборда (стр. 1)</strong><br>
  <img src="assets/dashboard_1.png" alt="Основной дашборд" width="70%">
    
  <strong>Пример дашборда (стр. 2)</strong><br>
  <img src="assets/dashboard_2.png" alt="Основной дашборд" width="70%">
</div>

## Лицензия
Этот проект распространяется под лицензией MIT. См. файл LICENSE для деталей.
## 🔗 Полезные ссылки
* [Документация VK API](https://dev.vk.com/ru/reference)
* [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
* [Metabase Documentation](https://www.metabase.com/docs/latest/)
* [Docker Documentation](https://docs.docker.com/get-started/)
  



