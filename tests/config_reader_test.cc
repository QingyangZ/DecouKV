#include "yaml-cpp/emitter.h"
#include "yaml-cpp/emittermanip.h"
#include "yaml-cpp/yaml.h"
#include "gtest/gtest.h"

TEST(YAMLTest, TestHello){
   YAML::Emitter out;
   out << "Hello, World!";
   std::string expectedStr = "Hello, World!";
   ASSERT_STREQ(expectedStr.c_str(), out.c_str());
}

TEST(TAMLTest, TestMap){
  YAML::Emitter out;
  out << YAML::BeginMap;
  out << YAML::Key << "name";
  out << YAML::Value << "Zach";
  out << YAML::Key << "position";
  out << YAML::Value << "LF";
  out << YAML::EndMap;
  std::string expectedStr;
  expectedStr = "name: Zach\nposition: LF";
  ASSERT_STREQ(expectedStr.c_str(), out.c_str());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
