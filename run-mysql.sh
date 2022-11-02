clear
#cat ./banner.txt

if lsof -Pi :43306 -sTCP:LISTEN -t >/dev/null ; then
    echo "Error: the db listening port is already taken, aborting ..."
    exit 1
fi

if [ -z ${MYSQL_UID+x} ]; 
then 
  echo "var is MYSQL_UID unset, setting ...";
  MYSQL_UID=$(id -u)
fi

if [ -z ${MYSQL_GID+x} ]; 
then 
  echo "var is MYSQL_GID unset, setting ...";
  MYSQL_GID=$(id -g)
fi

echo "Configuring db for: $MYSQL_UID:$MYSQL_GID ..."

mkdir -pv ./kdb

echo "Starting docker-compose ..."

MYSQL_UID=$MYSQL_UID docker-compose -f docker-compose-mysql.yml up --build
