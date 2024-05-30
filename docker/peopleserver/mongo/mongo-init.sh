echo "Creating curator user..."
echo '
    use '${OAR_MONGODB_DBNAME}'
    db.createUser(
        {
            user: "'${OAR_MONGODB_USER}'",
            pwd: "'${OAR_MONGODB_PASS}'",
            roles: [ "readWrite" ]
        }
    )
    exit' | mongo
