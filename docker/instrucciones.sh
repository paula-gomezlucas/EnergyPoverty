#MYSQL
docker run --detach --name=PobrezaElectrica-mysql -p 3306:3306 --env="MYSQL_ROOT_PASSWORD=root" mysql

# Hacer más cosas con ese contenedor
docker build . -f ./web.Dockerfile -t pe-web-image
docker run -d -p 80:80 --name pe-web-container pe-web-image

# y si quiero varios contenedores
docker-compose -f docker-compose.yml up --detachs