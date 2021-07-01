INCLUDE(FetchContent)

FetchContent_Declare(avro
    GIT_REPOSITORY    git@github.com:apache/avro.git
)
MESSAGE("Downloading dependencies..")
FetchContent_MakeAvailable(avro)
MESSAGE("Completed to download dependencies.")

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
ADD_EXECUTABLE(avro-c avro-c.c)
TARGET_LINK_LIBRARIES(avro-c avro)

