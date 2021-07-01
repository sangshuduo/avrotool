MESSAGE("Downloading dependencies..")
FetchContent_MakeAvailable(avro)
MESSAGE("Completed to download dependencies.")

INCLUDE(ExternalProject)
ExternalProject_Add(
    apache-avro
    PREFIX              "avro"
    SOURCE_DIR          ${PROJECT_BINARY_DIR}/_deps/avro-src/lang/c
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND   cmake -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}/build/
)
