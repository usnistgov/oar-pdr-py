version: "3.5"

services:
  mongo:
    image: mongo:latest
    container_name: mongo
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin
    ports:
      - "0.0.0.0:27017:27017"
    networks:
      - MONGO
    volumes:
      - type: volume
        source: MONGO_DATA
        target: /data/db
      - type: volume
        source: MONGO_CONFIG
        target: /data/configdb
  mongo-express:
    image: mongo-express:latest
    container_name: mongo-express
    environment:
      ME_CONFIG_MONGODB_ADMINUSERNAME: admin
      ME_CONFIG_MONGODB_ADMINPASSWORD: admin
      ME_CONFIG_MONGODB_SERVER: mongo
      ME_CONFIG_MONGODB_PORT: "27017"
    ports:
      - "0.0.0.0:8081:8081"
    networks:
      - MONGO
    depends_on:
      - mongo

networks:
  MONGO:
    name: MONGO

volumes:
  MONGO_DATA:
    name: MONGO_DATA
  MONGO_CONFIG:
    name: MONGO_CONFIG
