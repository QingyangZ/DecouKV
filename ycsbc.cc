//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <ratio>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include <atomic>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db.h"
#include "db/db_factory.h"
#include "histogram.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
std::atomic<int> cnt(0);
// 标志变量，用于控制线程停止
std::atomic<bool> stop_thread(false);
int timer_count = 0;
// 计数线程：每秒输出一次count的值并清零
void print_and_reset_count() {
  while (!stop_thread.load()) {
    // 每秒输出一次
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // 输出 count 的值
    std::cout << timer_count++ << ' ' << cnt.load() << std::endl;

    // 清零 count
    cnt.store(0);
  }
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
int ReadClient(shared_ptr<ycsbc::DB> db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading,shared_ptr<utils::Histogram> histogram, bool is_read) {
  db->Init();
  utils::Timer<utils::t_microseconds> timer;
  ycsbc::Client client(db, *wl);
  int oks = 0;
  if (is_read) {
    for (int i = 0; i < num_ops; ++i) {
      timer.Start();
      oks += client.TransactionRead();
      double duration = timer.End();
      histogram->Add_Fast(duration);
    } 
  } else {
    for (int i = 0; i < num_ops; ++i) {
      timer.Start();
      oks += client.TransactionInsert();
      double duration = timer.End();
      histogram->Add_Fast(duration);
    } 
  }

  db->Close();
  return oks;
}

int DelegateClient(shared_ptr<ycsbc::DB> db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading,shared_ptr<utils::Histogram> histogram) {
  db->Init();
  utils::Timer<utils::t_microseconds> timer;
  ycsbc::Client client(db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    timer.Start();
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
    double duration = timer.End();
    histogram->Add_Fast(duration);
    cnt.fetch_add(1, std::memory_order_relaxed);  // 原子加1
  }
  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  shared_ptr<ycsbc::DB> db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  vector<future<int>> actual_ops;
  vector<shared_ptr<utils::Histogram>> histogram_list;
  utils::Timer<utils::t_microseconds> timer;
  // 启动计数和输出线程
  // std::thread printer(print_and_reset_count);

  long long int total_ops = 0;
  int sum = 0;
  if (props.GetProperty("skipload") != "true"){
    // Loads data
    timer.Start();
    total_ops = stoll(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    for (int i = 0; i < num_threads; ++i) {
      auto histogram_tmp = make_shared<utils::Histogram>(utils::RecordUnit::h_microseconds);
      histogram_list.push_back(histogram_tmp);
      actual_ops.emplace_back(async(launch::async,
            DelegateClient, db, &wl, total_ops / num_threads, true, histogram_tmp));

    }
    assert((int)actual_ops.size() == num_threads);

    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    double duration = timer.End();
    utils::Histogram histogram(utils::RecordUnit::h_microseconds);
    for (auto &h : histogram_list){
        histogram.Merge(*h);
    }
    cerr << "# Loading records:\t" << sum << endl;
    cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cerr << "Throughput: " << total_ops / (duration / histogram.GetRecordUnit()) << " OPS" << endl;
    cerr << histogram.ToString() << endl;
  }else {
    cerr << "# Skipped load records!" << endl;
  }
  
  // Peforms transactions
  actual_ops.clear();
  histogram_list.clear();
  total_ops = stoll(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  timer.Start();
  // rw client
  if (0) {
    for (int i = 0; i < num_threads; ++i) {
      auto histogram_tmp = make_shared<utils::Histogram>(utils::RecordUnit::h_microseconds);
      histogram_list.push_back(histogram_tmp);
      if (i % 2 == 0) 
        actual_ops.emplace_back(async(launch::async,
              ReadClient, db, &wl, total_ops / num_threads, false, histogram_tmp, true));
      else 
        actual_ops.emplace_back(async(launch::async,
            ReadClient, db, &wl, total_ops / num_threads, false, histogram_tmp, false));
    }
  } else {
    for (int i = 0; i < num_threads; ++i) {
      auto histogram_tmp = make_shared<utils::Histogram>(utils::RecordUnit::h_microseconds);
      histogram_list.push_back(histogram_tmp);
      actual_ops.emplace_back(async(launch::async,
            DelegateClient, db, &wl, total_ops / num_threads, false, histogram_tmp));
    }
  }
    
  assert((int)actual_ops.size() == num_threads);
  
  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  utils::Histogram histogram(utils::RecordUnit::h_microseconds);
  for (auto &h : histogram_list){
    histogram.Merge( *h );
  }
  // 设置停止标志
  // stop_thread.store(true);
  auto duration = timer.End();
  cerr << "# Transaction throughput (KTPS)" << endl;
  cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cerr << "Throughput: " << total_ops / (duration / histogram.GetRecordUnit()) << " OPS" << endl;
  cerr << histogram.ToString() << endl;
  stop_thread.store(true);
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    }else if (strcmp(argv[argindex], "-dbpath") == 0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbpath", argv[argindex]);
      argindex++;
    }else if (strcmp(argv[argindex], "-skipload") == 0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("skipload", argv[argindex]);
      argindex++;
    }else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -dbpath: specify the path of DB location" << endl;
  cout << "  -skipload: whether to run the load phase, sometime you can run on an existing database" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void NewClient() {

}