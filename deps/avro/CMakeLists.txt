MESSAGE("Downloading dependencies..")
FetchContent_MakeAvailable(avro)
MESSAGE("Completed to download dependencies.")

INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}/_deps/avro-src/lang/c/src)
