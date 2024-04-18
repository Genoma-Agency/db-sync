# db-sync

Mysql/Mariadb records transfer utility.

This application copy/sync records between databases.

The selection of records to copy is made based on the primary key of the records.

## Warning

#### This application modifies database tables content. 

#### Use with extreme caution and back up databases before running.

This application does not change the structure of the tables. 
All tables in the source database must already be present and identical in the destination database.

## Usage

TODO

## Requirements

- C++20 compiler
- soci mysql
- lib4cxx
- boost (date_time program_options filesystem)
- fmt

## Build

### Rocky Linux 9

install required packages

```
dnf install g++ cmake git zip
dnf --enablerepo=crb install mysql-devel
dnf install boost-devel fmt-devel apr-devel apr-util-devel
```

soci-devel package is broken, build yourself

```
git clone https://github.com/SOCI/soci.git
cd soci
mkdir build
cd build/
cmake ..
make
make install
make clean
```

log4cxx-devel package isn't provided, build yourself

```
git clone https://github.com/apache/logging-log4cxx.git
cd logging-log4cxx
mkdir build
cd build/
cmake ..
make
make install
make clean
```

build db-sync

```
git clone https://github.com/Genoma-Agency/db-sync.git
cd db-sync

```
