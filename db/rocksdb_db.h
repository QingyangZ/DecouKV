//
//  rocksdb_db.h
//  YCSB-C
//
//  Created by zhenliu on 18/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#ifndef YCSB_C_ROCKSDB_DB_H
#define YCSB_C_ROCKSDB_DB_H

#include "core/db.h"
#include "core/config_reader.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace ycsbc {

  class RocksDB : public DB{
    public:
      RocksDB(const char *dbfilename);
      int Read(const std::string &table, const std::string &key,
          const std::vector<std::string> *fields,
          std::vector<KVPair> &result);

      int Scan(const std::string &table, const std::string &key,
          int len, const std::vector<std::string> *fields,
          std::vector<std::vector<KVPair>> &result);

      int Insert(const std::string &table, const std::string &key,
          std::vector<KVPair> &values);

      int Update(const std::string &table, const std::string &key,
          std::vector<KVPair> &values);

      int Delete(const std::string &table, const std::string &key);

      void printStats();

      ~RocksDB();

    private:
      rocksdb::DB *db_;
      rocksdb::Options options;
      unsigned no_found;

  }; //end of YCSB
} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H
