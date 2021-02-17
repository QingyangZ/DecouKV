//
//  leveldb_db.h
//  PROJECT NAME
//
//  Created by zhenliu on 05/02/2021.
//  Copyright (c) 2021 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#ifndef YCSB_C_LEVELDB_DB_H
#define YCSB_C_LEVELDB_DB_H

#include "core/db.h"
#include "leveldb/db.h"
#include "leveldb/options.h"

namespace ycsbc {
  class LevelDB : public DB {
    public:
      LevelDB(const char *dbfilename);
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

      ~LevelDB();

    private:
      leveldb::DB *db_;
      leveldb::Options options;
      unsigned no_found;

  }; //end of YCSB

}  //ycsbc

#endif //YCSB_C_LEVELDB_DB_H

