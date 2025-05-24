#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-std=c++17 -g -Wall -pthread ")

if(NOT DEFINED CMAKE_DB)
    set(CMAKE_DB "rocksdb")  # default rocksdb
endif()

#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include
  ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb0/include
  # ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/include
  # ${PROJECT_SOURCE_DIR}/db_impl/matrixkv/include
  )

#5. third path link path
link_directories(
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/build/
  ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib/
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb0/build/
  # ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/build
  # ${PROJECT_SOURCE_DIR}/db_impl/matrixkv
  )

if(CMAKE_DB STREQUAL "RocksDB")
  include_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/include
    )
  link_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/build
    )
elseif(CMAKE_DB STREQUAL "MatrixKV")
  include_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/matrixkv/include
    )
  link_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/matrixkv
    )
  add_definitions(-DUSE_MATRIXKV)
elseif(CMAKE_DB STREQUAL "ADOC")
  include_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/adoc/include
    )
  link_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/adoc
    )
elseif(CMAKE_DB STREQUAL "DecouKV")
  include_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/decoukv/include
    )
  link_directories(
    ${PROJECT_SOURCE_DIR}/db_impl/decoukv/build
    )
else()
  message(FATAL_ERROR "Unsupported DB type: ${CMAKE_DB}")
endif()

#6. source code file path
file(GLOB MAIN_FILE
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  )
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
  )

#7. define the executable
add_executable(${PROJECT_NAME} ${MAIN_FILE} ${SOURCE_FILE})
target_link_libraries(${PROJECT_NAME}
  pthread
  leveldb
  rocksdb
  yaml-cpp
  gtest
  snappy
  zstd
  lz4
  bz2
  z
  dl
  numa
  stdc++
  pmem
  uring
  crc32c
  )

#8. define gtest path
set(GTEST_LIBRARY ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib)
set(GTEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
set(GTEST_MAIN_LIBRARY ${GTEST_LIBRARY})

#9. compile test files
find_package(GTest)
enable_testing()
if (GTEST_FOUND)
  file(GLOB TEST_FILE ${PROJECT_SOURCE_DIR}/tests/*.cc)
  foreach(testsourcefile ${TEST_FILE})
    string(REGEX MATCH "[^/]+$" testsourcefilewithoutpath ${testsourcefile})
    string(REPLACE ".cc" "" testname ${testsourcefilewithoutpath})
    add_executable( ${testname} ${testsourcefile} ${SOURCE_FILE})
    target_link_libraries(${testname}
      yaml-cpp
      gtest
      leveldb
      rocksdb
      pthread
      snappy
      numa
      zstd
      lz4
      bz2
      z
      dl
      stdc++
      pmem
      uring
      crc32c
      )
    add_test(NAME ${testname} COMMAND ${testname})
  ENDFOREACH(testsourcefile ${TEST_FILE})
endif()

#10. generate compile commands
add_custom_command(TARGET ${PROJECT_NAME}
  POST_BUILD
  COMMAND bash -x ${PROJECT_SOURCE_DIR}/gen_compile_commands.sh
  VERBATIM
  )

