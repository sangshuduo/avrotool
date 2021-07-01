INCLUDE(FetchContent)

FetchContent_Declare(
    avro
    GIT_REPOSITORY    https://github.com/apache/avro.git
)

ADD_SUBDIRECTORY(avro)
