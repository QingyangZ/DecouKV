#include "yaml-cpp/emitter.h"
#include "yaml-cpp/emittermanip.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/node/parse.h"
#include "yaml-cpp/yaml.h"
#include "gtest/gtest.h"
#include <bits/stdint-uintn.h>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <fstream>
#include <sys/types.h>
#include <vector>

bool convert_to_string(std::string& target_size, uint64_t base_size){
  target_size.clear();
  if (base_size  >= 1<< 30){
    uint64_t prefix = base_size >> 30;
    target_size += std::to_string(prefix);
    target_size += "GB";
  }else if(base_size >= 1<< 20){
    uint64_t prefix = base_size >> 20;
    target_size += std::to_string(prefix);
    target_size += "MB";
  }else{
    return false;
  }
  return true;
}
uint64_t convert_to_base(std::string & target_size){
  size_t position_GB = target_size.find("GB");
  size_t position_MB = target_size.find("MB");
  uint64_t base_size = 0;
  if ( position_GB != std::string::npos){
    double prefix = std::stod(target_size.substr(0,target_size.size() -2 ));
    base_size += prefix * (1<< 30);
  }
  if ( position_MB != std::string::npos){
    double prefix = std::stod(target_size.substr(0,target_size.size() -2 ));
    base_size += prefix * (1<< 20);
  }
  return base_size;
}



struct db_config {
  uint64_t bloom_bits_;
  bool enable_direct_io_;
  bool enable_compaction_;
  uint64_t thread_compaction_;
  uint64_t block_cache_size_;
  uint64_t memtable_size_;
  uint64_t sst_file_size_;
};

template<>
struct YAML::convert<db_config>{
  static YAML::Node encode(const db_config& dc){
    Node node;
    node.push_back(dc.bloom_bits_);
    node.push_back(dc.enable_direct_io_);
    node.push_back(dc.enable_compaction_);
    node.push_back(dc.thread_compaction_);
    std::string block_cache_size;
    std::string memtable_size;
    std::string sst_file_size;
    convert_to_string(block_cache_size, dc.block_cache_size_);
    convert_to_string(memtable_size, dc.memtable_size_);
    convert_to_string(sst_file_size, dc.sst_file_size_);
    node.push_back(block_cache_size);
    node.push_back(memtable_size);
    node.push_back(sst_file_size);
    return node;
  }

  static bool decode(const Node& node, db_config& dc){
    if( !node.IsMap() || node.size()!= 7){
      return false;
    }
    dc.bloom_bits_ = node["bloom_bits"].as<uint64_t>();
    dc.enable_direct_io_ = node["enable_direct_io"].as<bool>();
    dc.enable_compaction_ = node["enable_compaction"].as<bool>();
    dc.thread_compaction_ = node["thread_compaction"].as<uint64_t>();
    std::string block_cache_size = node["block_cache_size"].as<std::string>();
    std::string memtable_size = node["memtable_size"].as<std::string>();
    std::string sst_file_size = node["sst_file_size"].as<std::string>();
    dc.block_cache_size_ = convert_to_base(block_cache_size);
    dc.memtable_size_ = convert_to_base(memtable_size);
    dc.sst_file_size_ = convert_to_base(sst_file_size);
    return true;
  }
};

TEST(YAMLTest, TestHello){
  YAML::Emitter out;
  out << "Hello, World!";
  std::string expectedStr = "Hello, World!";
  ASSERT_STREQ(expectedStr.c_str(), out.c_str());
}

TEST(YAMLTest, TestMap){
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

TEST(YAMLTest, TestReadDBConfig){
  YAML::Node lineup = YAML::LoadFile("../db_config.yaml");
  uint64_t expect_val = 10;
  for (auto it = lineup.begin(); it != lineup.end(); it++){
    db_config dc = it->second.as<db_config>();
    ASSERT_EQ(expect_val,dc.bloom_bits_);
  }

}
TEST(YAMLTest, TestConvertSize){
  std::string test1 = "100MB";
  uint64_t expect_base = 104857600;
  ASSERT_EQ(convert_to_base(test1),expect_base );
  std::string test2;
  uint64_t test2_base = 3;
  test2_base = test2_base << 30;
  convert_to_string(test2,test2_base);
  std::string expect_str = "3GB";
  ASSERT_STREQ(test2.c_str(), expect_str.c_str());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
