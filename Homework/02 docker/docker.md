sudo -i - �������� root-�����
apt-get install docker - ���������� ??? - � ������
snap info docker - ��������� ������
snap install docker - ������� � ����������
https://nuancesprog.ru/p/7481/
https://cloud.croc.ru/blog/about-technologies/flask-prilozheniya-v-docker/

��������� ������ ������������
https://stackoverflow.com/questions/47854463/docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socke
sudo usermod -a -G docker $USER
reboot
������������� ������

## �������� ������ � ������� Dockerfile
��� �������� ������������� Midnight Commander, ���� �� ������ �� �����������
apt install mc
mc - ���������

**��� �� ���� ��� ��
*������� ���������� � ������� ����� Dockerfile
*mkdir docker_flask
*��������� � ���������� � ������� Dockerfile
*cd docker_flask
*touch Dockerfile

�������������� ����� � ������ ������-������ �����, � ����� mc �������� ���������� ������ � ����� � ������� ���������� ��� ������
���� ����� �������� �� �� ����� �10

������� �����
docker build . -t ms/flask

������� �����
docker image rm "IMAGE ID"

���������� ����� ����� � ������ �������
docker image ls -a (�� ��� �)
0fe6decb72f9

������ ����������
docker run -d -p 8888:8888 --name my_flask_container ms/flask - ����� ���� ���������, ����������� ���
����� - ������, ������ - ����������
33 ������ �����
docker run -d -p 127.0.0.1:8888:80 --name flask_cont ms/flask

����� ������ ���� ���������� �����������
docker container ls

���������� ���������
docker container stop "CONTAINER ID"
docker container stop a0e071a7fc65
��������� ������ 
docker container start "CONTAINER ID"

������������� ���������
docker exec -it "CONTAINER ID" bash
28� ������ ������

netstat -ltupn - ��������� ������� �����
netstat -ltupn | grep 80
apt install net-tools - ���������� ���� ���


