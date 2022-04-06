cmake_minimum_required(VERSION 3.15)

project(avrotool
    LANGUAGES C
    VERSION 0.0.1
)

SET(CMAKE_VERBOSE_MAKEFILE ON)

add_subdirectory(deps)
add_subdirectory(src)

INSTALL(TARGETS avrotool
  RUNTIME DESTINATION bin
)

IF (${CMAKE_BUILD_TYPE} MATCHES "DEBUG")
    install(DIRECTORY ${CMAKE_BINARY_DIR}/build/lib DESTINATION /usr/local)
ENDIF ()

install(CODE "execute_process(COMMAND ldconfig)")
