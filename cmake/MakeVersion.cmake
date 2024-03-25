find_package(Git REQUIRED)

execute_process(
    COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
message(GIT_HASH="${GIT_HASH}")

execute_process(
    COMMAND ${GIT_EXECUTABLE} describe --tags --dirty --all
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_DESC
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
message(GIT_DESC="${GIT_DESC}")

set(GITVERSION_SOURCES ${PROJECT_SOURCE_DIR}/source/version.cpp)
message(GITVERSION_SOURCES="${GITVERSION_SOURCES}")

message(GIT_HASH="${GIT_HASH}")
message(GIT_DESC="${GIT_DESC}")

configure_file(${PROJECT_SOURCE_DIR}/src/version.cpp.in ${CMAKE_CURRENT_BINARY_DIR}/generated/version.cpp)

# aggiungo target solo quando esegui da cmake e non da target GitVersion
if (NOT DEFINED NO_CREATE_TARGET)
    message("adding target GitVersion")
    add_custom_target(GitVersion
      ${CMAKE_COMMAND}
      -D NO_CREATE_TARGET=1
      -D PROJECT_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
      -D CMAKE_PROJECT_NAME="${CMAKE_PROJECT_NAME}"
      -D CMAKE_PROJECT_VERSION="${CMAKE_PROJECT_VERSION}"
      -D CMAKE_PROJECT_VERSION_MAJOR="${CMAKE_PROJECT_VERSION_MAJOR}"
      -D CMAKE_PROJECT_VERSION_MINOR="${CMAKE_PROJECT_VERSION_MINOR}"
      -D CMAKE_PROJECT_VERSION_PATCH="${CMAKE_PROJECT_VERSION_PATCH}"
      -P ${PROJECT_SOURCE_DIR}/cmake/MakeVersion.cmake
    )
endif()

