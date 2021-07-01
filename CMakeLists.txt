cmake_minimum_required(VERSION 3.15)
project(avro-c
    LANGUAGES C
    VERSION 0.0.1
)

add_subdirectory(src)

INCLUDE(FetchContent)

FetchContent_Declare(avro
    GIT_REPOSITORY    https://github.com/apache/avro.git
)
MESSAGE("Downloading dependencies..")
FetchContent_MakeAvailable(avro)
MESSAGE("Completed to download dependencies.")

