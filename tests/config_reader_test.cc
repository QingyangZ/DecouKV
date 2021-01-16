#include "yaml-cpp/yaml.h"
#include "gtest/gtest.h"

TEST(YAMLTest, Hello){
   YAML::Emitter out;
   out << "Hello, World!";
   std::cout << "Here's the output YAML:\n" << out.c_str(); // prints "Hello, World!"
   ASSERT_EQ(0,0);
}
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
