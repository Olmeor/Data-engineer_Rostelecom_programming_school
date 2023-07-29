sudo -i - включить root-права
apt-get install docker - установить ??? - с лекции
snap info docker - поглядеть версии
snap install docker - скачать и установить
https://nuancesprog.ru/p/7481/
https://cloud.croc.ru/blog/about-technologies/flask-prilozheniya-v-docker/

Прописать доступ пользователю
https://stackoverflow.com/questions/47854463/docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socke
sudo usermod -a -G docker $USER
reboot
перезапустить убунту

## Создание образа с помощью Dockerfile
для удобства устанавливаем Midnight Commander, Азат на лекции им пользовался
apt install mc
mc - запускаем

**Это не надо для ДЗ
*создаем директорию в которой будет Dockerfile
*mkdir docker_flask
*переходим в директорию и создаем Dockerfile
*cd docker_flask
*touch Dockerfile

Расспаковываем архив в корень какого-нибудь диска, и через mc копируем содержимое архива в папку в которой находишься или нужную
Дале можно выходить из МС через Ф10

создаем образ
docker build . -t ms/flask

удалить образ
docker image rm "IMAGE ID"

посмотреть новый образ в списке образов
docker image ls -a (но без а)
0fe6decb72f9

запуск контейнера
docker run -d -p 8888:8888 --name my_flask_container ms/flask - порты надо проверить, неправильно тут
слева - машина, справа - контейнеры
33 минута порты
docker run -d -p 127.0.0.1:8888:80 --name flask_cont ms/flask

вывод списка всех запущенных контейнеров
docker container ls

остановить контейнер
docker container stop "CONTAINER ID"
docker container stop a0e071a7fc65
запустить заново 
docker container start "CONTAINER ID"

редактировать контейнер
docker exec -it "CONTAINER ID" bash
28я минута лекции

netstat -ltupn - поглядеть занятые порты
netstat -ltupn | grep 80
apt install net-tools - установить если нет


