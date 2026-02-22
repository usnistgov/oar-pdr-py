echo "Creating MIDAS user..."
echo '
    use '${OAR_MONGODB_DBNAME}'
    db.createUser(
        {
            user: "'${OAR_MONGODB_USER}'",
            pwd: "'${OAR_MONGODB_PASS}'",
            roles: [ "readWrite" ]
        }
    )
    exit' | mongosh -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD
