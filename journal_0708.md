##connect ubuntu
ssh ubuntu@13.217.39.47 -i /home/matias.sanchez/CS0055422/matias-key.pem

# DRY RUN
mysqlsh --js -h 127.0.0.1 -P 43254 -u test -ptest \
  -e "util.loadDump('/home/matias.sanchez/CS0055422/dump/logical_dump_channels_member_20250609', {
        includeTables: ['vt_byuser.channels_members'],
        showProgress: true,
        ignoreVersion: true,
        dryRun: true
      })"

# ACTUAL LOAD
mysqlsh --js -h 127.0.0.1 -P 43254 -u test -ptest \
  -e "util.loadDump('/home/matias.sanchez/CS0055422/dump/logical_dump_channels_member_20250609', {
        includeTables: ['vt_byuser.channels_members'],
        showProgress: true,
        ignoreVersion: true
      })"


## sandbox import

cd ~/sandboxes/msb_8_0_40
./use -uroot -e "SET PERSIST local_infile = ON;"
./restart

local_infile=ON

# Safer-but-fast defaults
innodb_flush_log_at_trx_commit=0
sync_binlog=0
innodb_flush_neighbors=0
innodb_io_capacity=4000
innodb_io_capacity_max=8000
innodb_read_io_threads=8
innodb_write_io_threads=8
innodb_log_buffer_size = 268435456
innodb_redo_log_capacity = 1G
skip-log-bin


mkdir -p /home/matias.sanchez/CS0055422/dump
tar -xvzf /srv/ftp/CS0055422/logical_dump_channels_member_20250609.tar.gz -C /home/matias.sanchez/CS0055422/dump

## export back
mysqldump -h 127.0.0.1 -P 43254 -u test -ptest \
  --single-transaction --quick --skip-lock-tables \
  vt_byuser channels_members | gzip > channels_members.sql.gz

scp -i /home/matias.sanchez/CS0055422/matias-key.pem channels_members.sql.gz ubuntu@13.217.39.47:/home/ubuntu/


mysql -e"create database vt_byuser;"
zcat channels_members.sql.gz | mysql -A vt_byuser 


create user 'msandbox'@'%'  identified by 'msandbox';
grant all privileges on *.* to 'msandbox'@'%';

drop user 'msandbox'@'%';

mysql -h127.0.0.1 -P3306 -umsandbox -pmsandbox

## install python 3.12

sudo apt update
sudo apt install -y software-properties-common build-essential libssl-dev zlib1g-dev \
    libncurses5-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

sudo apt install -y python3.12 python3.12-venv python3.12-dev

sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2
sudo update-alternatives --config python3

# Install the MySQL connector for Python 3.12
python3.12 -m pip install mysql-connector-python

# copy it to tmpfs for space constraints

# Stop MySQL
sudo systemctl stop mysql

# Move the table .ibd file to tmpfs
sudo mkdir -p /dev/shm/mysqltmp/vt_byuser
sudo mv /var/lib/mysql/vt_byuser/channels_members.ibd /dev/shm/mysqltmp/vt_byuser/
sudo ln -s /dev/shm/mysqltmp/vt_byuser/channels_members.ibd /var/lib/mysql/vt_byuser/channels_members.ibd

# Start MySQL
sudo systemctl start mysql

## Changing lower_case_table_names = 1

sudo rm -Rf /var/lib/mysql
sudo mkdir /var/lib/mysql
sudo chown mysql:mysql /var/lib/mysql

## pip for cryptography at pymysql

# 1) Make sure pip can pull wheels
python3.12 -m pip install --upgrade pip setuptools wheel

# 2) Install cffi + cryptography for *Python 3.12*
python3.12 -m pip install --upgrade cffi cryptography