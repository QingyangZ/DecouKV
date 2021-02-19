//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

namespace ycsbc {
  DB* DBFactory::CreateDB(utils::Properties &props) {
    if (props["dbname"] == "basic") {
      return new BasicDB;
    } else if (props["dbname"] == "lock_stl") {
      return new LockStlDB;
    } else if (props["dbname"] == "leveldb") {
      return new LevelDB(props["dbpath"].c_str());
    } else if (props["dbname"] == "rocksdb") {
      return new RocksDB(props["dbpath"].c_str());
    } else return NULL;
  }

}
