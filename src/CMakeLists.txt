SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)

INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}/build/include)
LINK_DIRECTORIES(${PROJECT_BINARY_DIR}/build/lib)

IF (NOT WIN32)
    STRING(ASCII 27 Esc)
    SET(ColourReSET "${Esc}[m")
    SET(ColourBold  "${Esc}[1m")
    SET(Red         "${Esc}[31m")
    SET(Green       "${Esc}[32m")
    SET(Yellow      "${Esc}[33m")
    SET(Blue        "${Esc}[34m")
    SET(Magenta     "${Esc}[35m")
    SET(Cyan        "${Esc}[36m")
    SET(White       "${Esc}[37m")
    SET(BoldRed     "${Esc}[1;31m")
    SET(BoldGreen   "${Esc}[1;32m")
    SET(BoldYellow  "${Esc}[1;33m")
    SET(BoldBlue    "${Esc}[1;34m")
    SET(BoldMagenta "${Esc}[1;35m")
    SET(BoldCyan    "${Esc}[1;36m")
    SET(BoldWhite   "${Esc}[1;37m")
ENDIF ()

IF (${COVER} MATCHES "true")
    MESSAGE("${Green} build with coverage ${ColourReSET}")
    SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
    SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
ENDIF ()

ADD_EXECUTABLE(avrotool avrotool.c)

SET(OS_ID "")
EXECUTE_PROCESS (
    COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    OUTPUT_VARIABLE OS_ID
    )

IF (${CMAKE_BUILD_TYPE} MATCHES "DEBUG")
    MESSAGE("${Yellow} DEBUG mode ${ColourReSET}")
    SET(CMAKE_C_FLAGS "-static-libasan -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment -O0 -g3 -DDEBUG ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

    TARGET_LINK_LIBRARIES(avrotool PRIVATE avro jansson)

ELSE ()
    MESSAGE("${Green} RELEASE mode use static avro library to link for release ${ColourReset}")
    SET(CMAKE_C_FLAGS "-DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

    ADD_LIBRARY(avro STATIC IMPORTED)
    IF(${OS_ID} MATCHES "centos")
        SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/build/lib64/libavro.a")
    ELSE()
        SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/build/lib/libavro.a")
    ENDIF()

    TARGET_LINK_LIBRARIES(avrotool PRIVATE avro jansson snappy lzma z)
ENDIF (${CMAKE_BUILD_TYPE} MATCHES "DEBUG")

