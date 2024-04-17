# db-sync

## Warning

### This application modifies database tables. Use with extreme caution and back up databases before running.

## Requiremens

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
