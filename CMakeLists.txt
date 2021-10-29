cmake_minimum_required(VERSION 3.15)

project(avrotool
    LANGUAGES C
    VERSION 0.0.1
)

add_subdirectory(deps)
add_subdirectory(src)

INSTALL(TARGETS avrotool
  RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_BINARY_DIR}/build/lib DESTINATION /usr/local)
install(CODE "execute_process(COMMAND ldconfig)")
