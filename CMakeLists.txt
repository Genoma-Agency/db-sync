cmake_minimum_required(VERSION 3.14)

project(db-sync VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SOCI REQUIRED COMPONENTS soci_core soci_mysql)
find_package(Boost 1.75 REQUIRED COMPONENTS date_time program_options filesystem)
find_package(log4cxx REQUIRED)
find_package(fmt REQUIRED)

set(GENERATED ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED})
message("generated directory: ${GENERATED}")
include(cmake/MakeVersion.cmake)

include_directories(
    include
    ${LOG4CXX_INCLUDE_DIRS}
)

file(GLOB APP_HEADERS ${PROJECT_SOURCE_DIR}/include/*.h ${PROJECT_SOURCE_DIR}/include/*.hxx)
list(JOIN APP_HEADERS "\n" STR_HEADERS)
message("APP_HEADERS:\n${STR_HEADERS}")

file(GLOB APP_SOURCES ${PROJECT_SOURCE_DIR}/src/*.cpp)
list(JOIN APP_SOURCES "\n" STR_SOURCES)
message("APP_SOURCES:\n${STR_SOURCES}")

file(GLOB GEN_SOURCES ${GENERATED}/*.cpp)
list(JOIN GEN_SOURCES "\n" STR_GEN)
message("GEN_SOURCES:\n${STR_GEN}")

if(CMAKE_BUILD_TYPE STREQUAL Debug)
  add_definitions(-DDEBUG)
endif()

add_executable(db-sync
    ${APP_HEADERS}
    ${APP_SOURCES}
    ${GEN_SOURCES}
)
target_link_libraries(db-sync
    log4cxx
    fmt::fmt
    SOCI::soci_core
    SOCI::soci_mysql
    ${Boost_LIBRARIES}
)

include(GNUInstallDirs)
install(TARGETS db-sync
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
