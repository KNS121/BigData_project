#!/bin/bash


if ! command -v docker &> /dev/null
then
    echo "Docker не установлен. Установка Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
else
    echo "Docker уже установлен."
fi


if ! command -v docker-compose &> /dev/null
then
    echo "Docker Compose не установлен. Установка Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
else
    echo "Docker Compose уже установлен."
fi

sudo docker-compose up --build -d --scale worker=2
echo "Docker start with CITUS ( 2 workers ) and PYTHON APP"


