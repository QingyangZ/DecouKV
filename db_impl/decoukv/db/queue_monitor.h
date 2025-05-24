#include <iostream>
#include <string>
#include <deque>
#include <math.h>
#include <chrono>
#include "db/memtable.h"

namespace ROCKSDB_NAMESPACE {

struct QueueMonitor {
  using time_point = std::chrono::_V2::system_clock::time_point;

  class QueueState {
   public:
    bool cal_start_ = false;
    bool cal_done_ = false;
    time_point arrive_;
    time_point service_;
    double arrive_rate_ = 0;
    double service_rate_ = 0;
    enum QueueLoadState { MinorOverload, SevereOverload, Underload, Normal };
    QueueLoadState state;

    void UpdateState() {
      if (arrive_rate_ > service_rate_ * 1.5) {
        state = SevereOverload;
      } else if (arrive_rate_ > service_rate_ * 1.1) {
        state = MinorOverload;
      } else if (arrive_rate_ < service_rate_ * 0.6) {
        state = Underload;
      } else {
        state = Normal;
      }
    }

    double CalLossRate(int N) { 
      double row = arrive_rate_ / service_rate_;
      return pow(row, N) * (1 - row) / (1 - pow(row, N + 1));
    }
    double CalQueueLength(int N) {
      double row = arrive_rate_ / service_rate_;
      return 1 / (1 - row) - (N + 1) * pow(row, N + 1) / (1 - pow(row, N + 1));
    }
    double CalWaitTime(int N, double queue_length) {
      double row = arrive_rate_ / service_rate_;
      double p0 = (1 - row) / (1 - pow(row, N + 1));
      return queue_length / (service_rate_ * 1 - p0);
    }
  };

  QueueState l0_queue;
  QueueState mem_queue;

  time_point start_time;
  int l0_service_num_ = 0;
  int L0_SERVICE_NUM = 10;

  // double l0_arrive_rate_ = 0;
  // bool l0_arrive_done = false;
  // double l0_service_rate_ = 0;
  bool l0_write_stall = false;

  double DeltaTime(time_point start, time_point end) {
    std::chrono::duration<double> runtime{end - start};
    return runtime.count();
  }
  double CalRate(time_point start, time_point end, int num) {
    return (double)num / std::chrono::duration<double>{end - start}.count();
  }
};

class KeyTable {
 public:
  // explicit KeyTable(const InternalKeyComparator& comparator,
  //                   const ImmutableMemTableOptions& options_);
  explicit KeyTable(const InternalKeyComparator& comparator);

  KeyTable(const KeyTable&) = delete;
  KeyTable& operator=(const KeyTable&) = delete;

  ~KeyTable();	// When compaction is completed, should delete the old datatable

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. 
  //Is it safe to call when KeyTable is being modified???????????????
  //Can be replaced by a number, when compation is completed, we can recalculate it
  //used to judge if the db should compact
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the datatable.
  //
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  void Add(MemTable* mem);

  void Del(int n);

  // Add level-pointer
  const void* AddPtrWithNode(const ParsedInternalKey& key,
                             InternalIterator** n_iter,
                             InternalIterator** prev_iter);

  // Return true if key table is empty
  bool IsEmpty();

  // If datatable contains a value for key, store it in *value and return true.
  // If datatable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  // Some get operation will start with the jumpflag node instead of the start of skiplist
  bool Get(const LookupKey& key, std::string* value, std::string* timestamp,
           Status* s, MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, ReadCallback* callback = nullptr,
           bool* is_blob_index = nullptr, bool do_merge = true);

  Status Compact(KeyTable* smalltable, SequenceNumber snum);

 private:

  friend class DataTableIterator;
  friend class DataTableBackwardIterator;

  MemTable::KeyComparator comparator_;
  int refs_;

 public:
  std::deque<MemTable*> mems_;
  // ConcurrentArena* arena_;
  // MergeableBloom* bloom_;
  // mTable table_;
  // bool IsLastTable;
  // std::unique_ptr<MemTableRep> table_;
  // std::unique_ptr<MemTableRep> range_del_table_;
};
}