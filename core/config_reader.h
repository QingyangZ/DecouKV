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
#include "yaml-cpp/yaml.h"

namespace ycsbc {

struct db_config {
  uint64_t bloom_bits_;
  bool enable_direct_io_;
  bool enable_compaction_;
  bool thread_compaction_;
  uint64_t block_cache_size_;
  uint64_t memtable_size_;
  uint64_t block_write_size_;
};

class config_reader {
  public:
  config_reader() { }
  ~config_reader() { }
  bool read_config(std::string file_path){
    return true;
  }

  private:
  std::map<std::string, db_config&> db_config_lists;
};

} //ycsbc

#endif // YCSB_C_CONFIG_READER_H
