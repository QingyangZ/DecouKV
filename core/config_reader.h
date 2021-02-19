//
//  config_reader.h
//  YCSB-C
//
//  Created by Zhen Liu on 19/01/2021.
//  Copyright (c) 2021 ZhenLiu <liuzhenm@mail.ustc.edu.cn>.
//
//
#ifndef YCSB_C_CONFIG_READER_H
#define YCSB_C_CONFIG_READER_H

#include <string>
#include <map>
#include <utility>
#include "yaml-cpp/node/parse.h"
#include "yaml-cpp/yaml.h"

namespace ycsbc {

  struct db_config {
    uint64_t bloom_bits_;
    bool enable_direct_io_;
    bool enable_compaction_;
    uint64_t thread_compaction_;
    uint64_t block_cache_size_;
    uint64_t memtable_size_;
    uint64_t sst_file_size_;
  };

  inline bool convert_to_string(std::string& target_size, uint64_t base_size){
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
  inline uint64_t convert_to_base(std::string & target_size){
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

} // namespace ycsbc

namespace YAML {
  template<>
    struct convert<ycsbc::db_config>{
      static YAML::Node encode(const ycsbc::db_config& dc){
        Node node;
        node.push_back(dc.bloom_bits_);
        node.push_back(dc.enable_direct_io_);
        node.push_back(dc.enable_compaction_);
        node.push_back(dc.thread_compaction_);
        std::string block_cache_size;
        std::string memtable_size;
        std::string sst_file_size;
        ycsbc::convert_to_string(block_cache_size, dc.block_cache_size_);
        ycsbc::convert_to_string(memtable_size, dc.memtable_size_);
        ycsbc::convert_to_string(sst_file_size, dc.sst_file_size_);
        node.push_back(block_cache_size);
        node.push_back(memtable_size);
        node.push_back(sst_file_size);
        return node;
      }

      static bool decode(const Node& node, ycsbc::db_config& dc){
        if( !node.IsMap()){
          return false;
        }
        dc.bloom_bits_ = node["bloom_bits"].as<uint64_t>();
        dc.enable_direct_io_ = node["enable_direct_io"].as<bool>();
        dc.enable_compaction_ = node["enable_compaction"].as<bool>();
        dc.thread_compaction_ = node["thread_compaction"].as<uint64_t>();
        std::string block_cache_size = node["block_cache_size"].as<std::string>();
        std::string memtable_size = node["memtable_size"].as<std::string>();
        std::string sst_file_size = node["sst_file_size"].as<std::string>();
        dc.block_cache_size_ = ycsbc::convert_to_base(block_cache_size);
        dc.memtable_size_ = ycsbc::convert_to_base(memtable_size);
        dc.sst_file_size_ = ycsbc::convert_to_base(sst_file_size);
        return true;
      }
    };
}

namespace ycsbc {
  class Config_Reader {
    public:
      Config_Reader(std::string file_path = "../db_config.yaml") {
        this->load_config(file_path);
      }
      ~Config_Reader() { }
      bool load_config(std::string file_path = "../db_config.yaml"){
        YAML::Node lineup = YAML::LoadFile(file_path);
        for (auto it = lineup.begin(); it != lineup.end(); it++){
          std::string dc_name = it->first.as<std::string>();
          db_config dc = it->second.as<db_config>();
          this->db_config_lists.insert(std::pair<std::string, db_config>(dc_name, std::move(dc)));
        }
        return true;
      }
      db_config& get_config(const std::string & db_name){
        auto it = this->db_config_lists.find(db_name);
        return it->second;
      }
    private:
      std::map<std::string, db_config> db_config_lists;
  };

} //ycsbc

#endif // YCSB_C_CONFIG_READER_H
