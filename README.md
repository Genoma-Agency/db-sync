# db-sync

Mysql/Mariadb records transfer utility.

This application copy/sync records between databases.

The selection of records to copy is made based on the primary key of the records. 

Optionally update of records can be enabled; to optimize speed and memory consumption the md5sum of the fields concatenation is used to compare records.

## Warning

#### This application modifies database tables content. 

#### Use with extreme caution and back up databases before running.

This application does not change the structure of the tables. 
All tables in the source database must already be present and identical in the destination database.

## Usage

```
$ ./db-sync 
Allowed arguments:
  -h [ --help ]                         print this help message
  -v [ --version ]                      print version
  -c [ --copy ]                         copy records from source to target
  -s [ --sync ]                         sync records from source to target
  -d [ --dry-run ]                      execute without modifying the target 
                                        database
  --update                              enable update of records from source to
                                        target
  --nofail                              don't stop if error on destination 
                                        records
  --disablebinlog                       disable binary log (privilege required)
  --fromHost arg                        source database host IP or name
  --fromPort arg (= 3306)               source database port
  --fromUser arg                        source database username
  --fromPwd arg                         source database password
  --fromSchema arg                      source database schema
  --toHost arg                          target database host IP or name
  --toPort arg (= 3306)                 target database port
  --toUser arg                          target database username
  --toPwd arg                           target database password
  --toSchema arg                        target database schema
  --tables arg                          tables to process (if none are 
                                        provided, use all tables)
  --logConfig arg (= ./db-sync-log.xml) path of logger xml configuration
  --pkBulk arg (= 10000000)             number of primary keys to read with a 
                                        single query
  --modifyBulk arg (= 5000)             number of records to read to 
                                        insert/update in a single transaction
```

### Modes

The application has two different operation modes: `sync` and `copy`.

The optional `update` flag enable the update of records with the same primary key but with fields with different values.

Sample source table

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 1             | B             | 0.34    | 2345    | source  |
| 2             | A             | NULL    | 3456    | source  |
| 2             | B             | 11.45   | 4567    | source  |
| 3             | A             | NULL    | 5678    | source  |

Sample target table

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 3             | A             | 58.25   | 9874    | changed |
| 3             | B             | 12.56   | 5678    | target  |


#### sync

With this mode the target tables have the same records after the execution; if a target table has records not present in the source table, they are deleted.

##### without `update`

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 1             | B             | 0.34    | 2345    | source  |
| 2             | A             | NULL    | 3456    | source  |
| 2             | B             | 11.45   | 4567    | source  |
| 3             | A             | 58.25   | 9874    | changed |

##### with `update`

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 1             | B             | 0.34    | 2345    | source  |
| 2             | A             | NULL    | 3456    | source  |
| 2             | B             | 11.45   | 4567    | source  |
| 3             | A             | NULL    | 5678    | source  |

#### copy

With this mode all the records in source tables are copied into target tables (if not already exist);  if a target table has records not present in the source table, they are preserved.

##### without `update`

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 1             | B             | 0.34    | 2345    | source  |
| 2             | A             | NULL    | 3456    | source  |
| 2             | B             | 11.45   | 4567    | source  |
| 3             | A             | 58.25   | 9874    | changed |
| 3             | B             | 12.56   | 5678    | target  |


##### with `update`

| Primary_Key_1 | Primary_Key_2 | Field_1 | Field_2 | Field_3 |
| ------------- | ------------- | ------- | ------- | ------- |
| 1             | A             | NULL    | 1234    | source  |
| 1             | B             | 0.34    | 2345    | source  |
| 2             | A             | NULL    | 3456    | source  |
| 2             | B             | 11.45   | 4567    | source  |
| 3             | A             | NULL    | 5678    | source  |
| 3             | B             | 12.56   | 5678    | target  |

### Performace

If you want speed, you need memory. If you want low memory usage, you need time.

To copy/sync a table the application loads all primary keys in memory from both source and target database to compare them.

Memory usage is controlled by two arguments:

- `pkBulk` 
- `modifyBulk`

To calculate the average maximum memory required you can use the formula:

- T = table with the highest number of rows (sum of source and target)
- ST = rows count of T in source
- TT = rows count of T in target
- PK = size of T primary key [C++ data type] (integer autoincrement is 4)
- TF = number of fields in T
- TS = average row size of T
- MI = (PK + 4) * (ST + TT)
- ML = `pkBulk` * 50
- MD = ( TF * 100 + TS ) * `modifyBulk`;

max memory = MI + min(ML, MD)

## Required libraries

- soci mysql
- lib4cxx
- boost (date_time program_options filesystem)
- fmt

## Build

C++20 compiler

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
