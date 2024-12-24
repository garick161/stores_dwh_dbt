#!/bin/bash

# Переходим директорию проекта и активируем виртуальное окружение
cd /root/stores_dwh_dbt/
source venv/bin/activate

# Переходим в директорию, где инициорован dbt проект
cd stores_dbt

# Указываем файл куда писать логи
log_file="../bash_scripts/crone_dbt.log"

echo "############################################################################################" >> "$log_file"
# Добавляем временную метку логов
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$log_file"
}

log "Запуск скрипта"

# Запускаем скрипт, который забирает записи из csv файлов посуточно и записывает базу данных - источник
cur_date=$(python ../py_stripts/daily_loader.py | tee -a "$log_file") # возвращает текущую дату

# Проверяем, успешно ли выполнен скрипт, если нет, выходим из выполнения
if [ $? -ne 0 ]; then
    log "Ошибка при выполнении загрузки данных в источник из csv"
    exit 1
fi

log "Дата загрузки: $cur_date"
# Форматируем дату окончания периода
# Добавляем 1 день к cur_date
next_date=$(date -d "$cur_date + 1 day" +%Y-%m-%d)

# # # Запускаем dbt build с переменными
dbt build --vars "{'date_from': '$cur_date', 'date_to': '$next_date'}" | tee -a "$log_file"

log "Работа скрипта завершена"

