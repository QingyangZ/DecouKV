#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 11)

#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include
  ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
)

link_directories(
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/build/*.a
  ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib/*.a
)

#5. source code file path
file(GLOB SRC
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
)
MESSAGE(test "dddd" ${SRC})
#6. define the executable
add_executable(${PROJECT_NAME} ${SRC})

target_link_libraries(${PROJECT_NAME} pthread )
