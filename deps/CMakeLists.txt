INCLUDE(FetchContent)

FetchContent_Declare(
    avro
    GIT_REPOSITORY    https://github.com/apache/avro.git
    GIT_TAG           release-1.11.0
)

ADD_SUBDIRECTORY(avro)
