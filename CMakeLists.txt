#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-std=c++11 -g -Wall -pthread")

#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include
  ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
  )

#5. third path link path
link_directories(
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/build/
  ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib/
  )

#6. source code file path
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
  )

#7. define the executable
add_executable(${PROJECT_NAME} ${SOURCE_FILE})
target_link_libraries(${PROJECT_NAME} pthread )

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
    add_executable( ${testname} ${testsourcefile})
    target_link_libraries(${testname}
      yaml-cpp
      gtest
      pthread
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

