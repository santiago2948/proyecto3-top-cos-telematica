# Instrucciones replicacion del proyecto 3:

## Despliegue:

Clonar el repositorio de github.
```{bash}
    git clone https://github.com/santiago2948/proyecto3-top-cos-telematica.git
```
Instalar docker en ubuntu:22.04:
```{bash}
    sudo apt update
    sudo apt install docker.io -y
    sudo apt install docker-compose -y

    sudo systemctl enable docker
    sudo systemctl start docker
```

Instalar python y venv en ubutu:

```{bash}
sudo apt install python3 python3-venv python3-pip -y
```

En el archivo docker-compose.yml se encuentra la configuracion para lanzar la base de datos SQL que vamos a utilizar en este proyecto como segunda fuente de datos, por lo que para lanzarla deberemos de correr el siguiente comando:

```{bash}
    docker-compose up -d
```