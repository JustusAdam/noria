ZK=/usr/share/zookeeper/bin/zkServer.sh
NORIA=$HOME/research/noria
NORIA_MYSQL=$HOME/research/noria-mysql
DEPL=1
BUILD_TYPE=debug

$ZK start

# using noria-mysql commit 284b2ae

cd $NORIA

cargo build
target/$BUILD_TYPE/noria-server --deployment $DEPL -v &

cd $NORIA_MYSQL

cargo build
target/$BUILD_TYPE/noria-mysql --deployment $DEPL &

cd $NORIA

target/$BUILD_TYPE/lobsters --queries ohua --notifications-only --prime
