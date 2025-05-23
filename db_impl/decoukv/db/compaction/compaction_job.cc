//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"

#include <algorithm>
#include <cinttypes>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_builder.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const char* GetCompactionReasonString(CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      return "Unknown";
    case CompactionReason::kLevelL0FilesNum:
      return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize:
      return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification:
      return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio:
      return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum:
      return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize:
      return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles:
      return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl:
      return "FIFOTtl";
    case CompactionReason::kManualCompaction:
      return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction:
      return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles:
      return "BottommostFiles";
    case CompactionReason::kTtl:
      return "Ttl";
    case CompactionReason::kFlush:
      return "Flush";
    case CompactionReason::kExternalSstIngestion:
      return "ExternalSstIngestion";
    case CompactionReason::kPeriodicCompaction:
      return "PeriodicCompaction";
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
      assert(false);
      return "Invalid";
  }
}

// Maintains state for each sub-compaction
struct CompactionJob::SubcompactionState {
  const Compaction* compaction;
  std::unique_ptr<CompactionIterator> c_iter;

  // The boundaries of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  Slice *start, *end;

  // The return status of this subcompaction
  Status status;

  // The return IO Status of this subcompaction
  IOStatus io_status;

  // Files produced by this subcompaction
  struct Output {
    Output(FileMetaData&& _meta, const InternalKeyComparator& _icmp,
           bool _enable_order_check, bool _enable_hash)
        : meta(std::move(_meta)),
          validator(_icmp, _enable_order_check, _enable_hash),
          finished(false) {}
    FileMetaData meta;
    OutputValidator validator;
    bool finished;
    std::shared_ptr<const TableProperties> table_properties;
  };

  // State kept for output being generated
  std::vector<Output> outputs;
  std::vector<BlobFileAddition> blob_file_additions;
  std::unique_ptr<WritableFileWriter> outfile;
  std::unique_ptr<TableBuilder> builder;

  Output* current_output() {
    if (outputs.empty()) {
      // This subcompaction's output could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particularly likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }

  uint64_t current_output_file_size = 0;

  // State during the subcompaction
  uint64_t total_bytes = 0;
  uint64_t num_output_records = 0;
  CompactionJobStats compaction_job_stats;
  uint64_t approx_size = 0;
  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;

  SubcompactionState(Compaction* c, Slice* _start, Slice* _end, uint64_t size)
      : compaction(c), start(_start), end(_end), approx_size(size) {
    assert(compaction != nullptr);
  }

  // Adds the key and value to the builder
  // If paranoid is true, adds the key-value to the paranoid hash
  Status AddToBuilder(const Slice& key, const Slice& value) {
    auto curr = current_output();
    assert(builder != nullptr);
    assert(curr != nullptr);
    Status s = curr->validator.Add(key, value);
    if (!s.ok()) {
      return s;
    }
    builder->Add(key, value);
    return Status::OK();
  }

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key, uint64_t curr_file_size) {
    const InternalKeyComparator* icmp =
        &compaction->column_family_data()->internal_comparator();
    const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

    // Scan to find earliest grandparent file that contains key.
    while (grandparent_index < grandparents.size() &&
           icmp->Compare(internal_key,
                         grandparents[grandparent_index]->largest.Encode()) >
               0) {
      if (seen_key) {
        overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
      }
      assert(grandparent_index + 1 >= grandparents.size() ||
             icmp->Compare(
                 grandparents[grandparent_index]->largest.Encode(),
                 grandparents[grandparent_index + 1]->smallest.Encode()) <= 0);
      grandparent_index++;
    }
    seen_key = true;

    if (overlapped_bytes + curr_file_size >
        compaction->max_compaction_bytes()) {
      // Too much overlap for current output; start new output
      overlapped_bytes = 0;
      return true;
    }

    return false;
  }
};

// Maintains state for the entire compaction
struct CompactionJob::CompactionState {
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing
  // key-range
  std::vector<CompactionJob::SubcompactionState> sub_compact_states;
  Status status;

  size_t num_output_files = 0;
  uint64_t total_bytes = 0;
  size_t num_blob_output_files = 0;
  uint64_t total_blob_bytes = 0;
  uint64_t num_output_records = 0;

  explicit CompactionState(Compaction* c) : compaction(c) {}

  Slice SmallestUserKey() {
    for (const auto& sub_compact_state : sub_compact_states) {
      if (!sub_compact_state.outputs.empty() &&
          sub_compact_state.outputs[0].finished) {
        return sub_compact_state.outputs[0].meta.smallest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }

  Slice LargestUserKey() {
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      if (!it->outputs.empty() && it->current_output()->finished) {
        assert(it->current_output() != nullptr);
        return it->current_output()->meta.largest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }
};

void CompactionJob::AggregateStatistics() {
  assert(compact_);

  for (SubcompactionState& sc : compact_->sub_compact_states) {
    auto& outputs = sc.outputs;

    if (!outputs.empty() && !outputs.back().meta.fd.file_size) {
      // An error occurred, so ignore the last output.
      outputs.pop_back();
    }

    compact_->num_output_files += outputs.size();
    compact_->total_bytes += sc.total_bytes;

    const auto& blobs = sc.blob_file_additions;

    compact_->num_blob_output_files += blobs.size();

    for (const auto& blob : blobs) {
      compact_->total_blob_bytes += blob.GetTotalBlobBytes();
    }

    compact_->num_output_records += sc.num_output_records;

    compaction_job_stats_->Add(sc.compaction_job_stats);
  }
}

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const FileOptions& file_options, VersionSet* versions,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum, LogBuffer* log_buffer,
    FSDirectory* db_directory, FSDirectory* output_directory,
    FSDirectory* blob_output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, std::shared_ptr<Cache> table_cache,
    EventLogger* event_logger, bool paranoid_file_checks, bool measure_io_stats,
    const std::string& dbname, CompactionJobStats* compaction_job_stats,
    Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<int>* manual_compaction_paused, const std::string& db_id,
    const std::string& db_session_id, std::string full_history_ts_low,
    BlobFileCompletionCallback* blob_callback)
    : job_id_(job_id),
      compact_(new CompactionState(compaction)),
      compaction_job_stats_(compaction_job_stats),
      compaction_stats_(compaction->compaction_reason(), 1),
      dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      db_options_(db_options),
      file_options_(file_options),
      env_(db_options.env),
      io_tracer_(io_tracer),
      fs_(db_options.fs, io_tracer),
      file_options_for_read_(
          fs_->OptimizeForCompactionTableRead(file_options, db_options_)),
      versions_(versions),
      shutting_down_(shutting_down),
      manual_compaction_paused_(manual_compaction_paused),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_directory_(output_directory),
      blob_output_directory_(blob_output_directory),
      stats_(stats),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      bottommost_level_(false),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats),
      write_hint_(Env::WLTH_NOT_SET),
      thread_pri_(thread_pri),
      full_history_ts_low_(std::move(full_history_ts_low)),
      blob_callback_(blob_callback) {
  assert(compaction_job_stats_ != nullptr);
  assert(log_buffer_ != nullptr);
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  ReportStartedCompaction(compaction);
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::ReportStartedCompaction(Compaction* compaction) {
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);

  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS,
      compaction->is_manual_compaction() +
          (compaction->deletion_compaction() << 1));

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

  compaction_job_stats_->is_manual_compaction =
      compaction->is_manual_compaction();
  compaction_job_stats_->is_full_compaction = compaction->is_full_compaction();
}

void CompactionJob::Prepare() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);

  // Generate file_levels_ for compaction before making Iterator
  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  if (c->ShouldFormSubcompactions()) {
    {
      StopWatch sw(db_options_.clock, stats_, SUBCOMPACTION_SETUP_TIME);
      GenSubcompactionBoundaries();
    }
    assert(sizes_.size() == boundaries_.size() + 1);

    for (size_t i = 0; i <= boundaries_.size(); i++) {
      Slice* start = i == 0 ? nullptr : &boundaries_[i - 1];
      Slice* end = i == boundaries_.size() ? nullptr : &boundaries_[i];
      compact_->sub_compact_states.emplace_back(c, start, end, sizes_[i]);
    }
    RecordInHistogram(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                      compact_->sub_compact_states.size());
  } else {
    constexpr Slice* start = nullptr;
    constexpr Slice* end = nullptr;
    constexpr uint64_t size = 0;

    compact_->sub_compact_states.emplace_back(c, start, end, size);
  }
}

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

void CompactionJob::GenSubcompactionBoundaries() {
  auto* c = compact_->compaction;
  auto* cfd = c->column_family_data();
  const Comparator* cfd_comparator = cfd->user_comparator();
  std::vector<Slice> bounds;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();

  // Add the starting and/or ending key of certain input files as a potential
  // boundary
  for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
    int lvl = c->level(lvl_idx);
    if (lvl >= start_lvl && lvl <= out_lvl) {
      const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
      size_t num_files = flevel->num_files;

      if (num_files == 0) {
        continue;
      }

      if (lvl == 0) {
        // For level 0 add the starting and ending key of each file since the
        // files may have greatly differing key ranges (not range-partitioned)
        for (size_t i = 0; i < num_files; i++) {
          bounds.emplace_back(flevel->files[i].smallest_key);
          bounds.emplace_back(flevel->files[i].largest_key);
        }
      } else {
        // For all other levels add the smallest/largest key in the level to
        // encompass the range covered by that level
        bounds.emplace_back(flevel->files[0].smallest_key);
        bounds.emplace_back(flevel->files[num_files - 1].largest_key);
        if (lvl == out_lvl) {
          // For the last level include the starting keys of all files since
          // the last level is the largest and probably has the widest key
          // range. Since it's range partitioned, the ending key of one file
          // and the starting key of the next are very close (or identical).
          for (size_t i = 1; i < num_files; i++) {
            bounds.emplace_back(flevel->files[i].smallest_key);
          }
        }
      }
    }
  }

  std::sort(bounds.begin(), bounds.end(),
            [cfd_comparator](const Slice& a, const Slice& b) -> bool {
              return cfd_comparator->Compare(ExtractUserKey(a),
                                             ExtractUserKey(b)) < 0;
            });
  // Remove duplicated entries from bounds
  bounds.erase(
      std::unique(bounds.begin(), bounds.end(),
                  [cfd_comparator](const Slice& a, const Slice& b) -> bool {
                    return cfd_comparator->Compare(ExtractUserKey(a),
                                                   ExtractUserKey(b)) == 0;
                  }),
      bounds.end());

  // Combine consecutive pairs of boundaries into ranges with an approximate
  // size of data covered by keys in that range
  uint64_t sum = 0;
  std::vector<RangeWithSize> ranges;
  // Get input version from CompactionState since it's already referenced
  // earlier in SetInputVersioCompaction::SetInputVersion and will not change
  // when db_mutex_ is released below
  auto* v = compact_->compaction->input_version();
  for (auto it = bounds.begin();;) {
    const Slice a = *it;
    ++it;

    if (it == bounds.end()) {
      break;
    }

    const Slice b = *it;

    // ApproximateSize could potentially create table reader iterator to seek
    // to the index block and may incur I/O cost in the process. Unlock db
    // mutex to reduce contention
    db_mutex_->Unlock();
    uint64_t size = versions_->ApproximateSize(SizeApproximationOptions(), v, a,
                                               b, start_lvl, out_lvl + 1,
                                               TableReaderCaller::kCompaction);
    db_mutex_->Lock();
    ranges.emplace_back(a, b, size);
    sum += size;
  }

  // Group the ranges into subcompactions
  const double min_file_fill_percent = 4.0 / 5;
  int base_level = v->storage_info()->base_level();
  uint64_t max_output_files = static_cast<uint64_t>(std::ceil(
      sum / min_file_fill_percent /
      MaxFileSizeForLevel(*(c->mutable_cf_options()), out_lvl,
          c->immutable_cf_options()->compaction_style, base_level,
          c->immutable_cf_options()->level_compaction_dynamic_level_bytes)));
  uint64_t subcompactions =
      std::min({static_cast<uint64_t>(ranges.size()),
                static_cast<uint64_t>(c->max_subcompactions()),
                max_output_files});
  if (subcompactions > 1) {
    double mean = sum * 1.0 / subcompactions;
    // Greedily add ranges to the subcompaction until the sum of the ranges'
    // sizes becomes >= the expected mean size of a subcompaction
    sum = 0;
    for (size_t i = 0; i + 1 < ranges.size(); i++) {
      sum += ranges[i].size;
      if (subcompactions == 1) {
        // If there's only one left to schedule then it goes to the end so no
        // need to put an end boundary
        continue;
      }
      if (sum >= mean) {
        boundaries_.emplace_back(ExtractUserKey(ranges[i].range.limit));
        sizes_.emplace_back(sum);
        subcompactions--;
        sum = 0;
      }
    }
    sizes_.emplace_back(sum + ranges.back().size);
  } else {
    // Only one range so its size is the total sum of sizes computed above
    sizes_.emplace_back(sum);
  }
}

Status CompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();

  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = db_options_.clock->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                             &compact_->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact_->sub_compact_states[0]);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }

  compaction_stats_.micros = db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.cpu_micros = 0;
  for (size_t i = 0; i < compact_->sub_compact_states.size(); i++) {
    compaction_stats_.cpu_micros +=
        compact_->sub_compact_states[i].compaction_job_stats.cpu_micros;
  }

  RecordTimeToHistogram(stats_, COMPACTION_TIME, compaction_stats_.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.cpu_micros);

  TEST_SYNC_POINT("CompactionJob::Run:BeforeVerify");

  // Check if any thread encountered an error during execution
  Status status;
  IOStatus io_s;
  bool wrote_new_blob_files = false;

  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      io_s = state.io_status;
      break;
    }

    if (!state.blob_file_additions.empty()) {
      wrote_new_blob_files = true;
    }
  }

  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->Fsync(IOOptions(), dbg);
    }

    if (io_s.ok() && wrote_new_blob_files && blob_output_directory_ &&
        blob_output_directory_ != output_directory_) {
      io_s = blob_output_directory_->Fsync(IOOptions(), dbg);
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    thread_pool.clear();
    std::vector<const CompactionJob::SubcompactionState::Output*> files_output;
    for (const auto& state : compact_->sub_compact_states) {
      for (const auto& output : state.outputs) {
        files_output.emplace_back(&output);
      }
    }
    ColumnFamilyData* cfd = compact_->compaction->column_family_data();
    auto prefix_extractor =
        compact_->compaction->mutable_cf_options()->prefix_extractor.get();
    std::atomic<size_t> next_file_idx(0);
    auto verify_table = [&](Status& output_status) {
      while (true) {
        size_t file_idx = next_file_idx.fetch_add(1);
        if (file_idx >= files_output.size()) {
          break;
        }
        // Verify that the table is usable
        // We set for_compaction to false and don't OptimizeForCompactionTableRead
        // here because this is a special case after we finish the table building
        // No matter whether use_direct_io_for_flush_and_compaction is true,
        // we will regard this verification as user reads since the goal is
        // to cache it here for further user reads
        ReadOptions read_options;
        InternalIterator* iter = cfd->table_cache()->NewIterator(
            read_options, file_options_, cfd->internal_comparator(),
            files_output[file_idx]->meta, /*range_del_agg=*/nullptr,
            prefix_extractor,
            /*table_reader_ptr=*/nullptr,
            cfd->internal_stats()->GetFileReadHist(
                compact_->compaction->output_level()),
            TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
            /*skip_filters=*/false, compact_->compaction->output_level(),
            MaxFileSizeForL0MetaPin(
                *compact_->compaction->mutable_cf_options()),
            /*smallest_compaction_key=*/nullptr,
            /*largest_compaction_key=*/nullptr,
            /*allow_unprepared_value=*/false);
        auto s = iter->status();

        if (s.ok() && paranoid_file_checks_) {
          OutputValidator validator(cfd->internal_comparator(),
                                    /*_enable_order_check=*/true,
                                    /*_enable_hash=*/true);
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            s = validator.Add(iter->key(), iter->value());
            if (!s.ok()) {
              break;
            }
          }
          if (s.ok()) {
            s = iter->status();
          }
          if (s.ok() &&
              !validator.CompareValidator(files_output[file_idx]->validator)) {
            s = Status::Corruption("Paranoid checksums do not match");
          }
        }

        delete iter;

        if (!s.ok()) {
          output_status = s;
          break;
        }
      }
    };
    for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
      thread_pool.emplace_back(verify_table,
                               std::ref(compact_->sub_compact_states[i].status));
    }
    verify_table(compact_->sub_compact_states[0].status);
    for (auto& thread : thread_pool) {
      thread.join();
    }
    for (const auto& state : compact_->sub_compact_states) {
      if (!state.status.ok()) {
        status = state.status;
        break;
      }
    }
  }

  TablePropertiesCollection tp;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.outputs) {
      auto fn =
          TableFileName(state.compaction->immutable_cf_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
  }
  compact_->compaction->SetOutputTableProperties(std::move(tp));

  // Finish up all book-keeping to unify the subcompaction results
  AggregateStatistics();
  UpdateCompactionStats();

  RecordCompactionIOStats();
  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");

  compact_->status = status;
  return status;
}

Status CompactionJob::Install(const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;

  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  assert(cfd);
  // std::cout << cfd->current()->storage_info()->NumLevelFiles(0) << std::endl;

  cfd->internal_stats()->AddCompactionStats(
      compact_->compaction->output_level(), thread_pri_, compaction_stats_);

  if (status.ok()) {
    status = InstallCompactionResults(mutable_cf_options);
  }
  // std::ofstream fout("L0Data.txt", std::ios::app);
  if (compact_->compaction->output_level() == 1) {
    // auto current_ = std::chrono::system_clock::now();
    //   // cfd->queue_moniter.PrintTime(cfd->queue_moniter.l0_service_, current_);
    // cfd->queue_moniter.l0_queue.service_rate_ = cfd->queue_moniter.CalRate(
    //     cfd->queue_moniter.l0_queue.service_, current_,
    //     compact_->compaction->num_input_files(0));

    // double l0_loss_rate = cfd->queue_moniter.l0_queue.CalLossRate(
    //     cfd->GetLatestMutableCFOptions()->level0_slowdown_writes_trigger);
    // double l0_queue_length = cfd->queue_moniter.l0_queue.CalQueueLength(
    //     cfd->GetLatestMutableCFOptions()->level0_slowdown_writes_trigger);

    // cfd->queue_moniter.l0_queue.UpdateState();

    // size_t write_buffer_size;
    // if (cfd->GetCurrentMutableCFOptions()->use_queue_monitor) {
    //   cfd->key_table->Del(compact_->compaction->num_input_files(0));
    //   switch (cfd->queue_moniter.l0_queue.state) {
    //     case QueueMonitor::QueueState::QueueLoadState::SevereOverload:
    //       db_options_.env->SetBackgroundThreads(24, Env::Priority::LOW);
    //       db_options_.env->SetBackgroundThreads(1, Env::Priority::HIGH);
    //       cfd->SetWriteStallParameter(20, 100, 180, 512 << 20);
    //       break;

    //     case QueueMonitor::QueueState::QueueLoadState::MinorOverload:
    //       db_options_.env->SetBackgroundThreads(
    //           (db_options_.env->GetBackgroundThreads(Env::Priority::LOW) > 20)
    //               ? 24
    //               : db_options_.env->GetBackgroundThreads(Env::Priority::LOW) +
    //                     4,
    //           Env::Priority::LOW);

    //       db_options_.env->SetBackgroundThreads(1, Env::Priority::HIGH);
    //       write_buffer_size =
    //           (2 * cfd->GetLatestMutableCFOptions()->write_buffer_size >
    //            512 << 20)
    //               ? 512 << 20
    //               : 2 * cfd->GetLatestMutableCFOptions()->write_buffer_size;
    //       cfd->SetWriteStallParameter(
    //           cfd->GetLatestMutableCFOptions()
    //                   ->level0_file_num_compaction_trigger +
    //               4,
    //           cfd->GetLatestMutableCFOptions()->level0_slowdown_writes_trigger +
    //               20,
    //           cfd->GetLatestMutableCFOptions()->level0_stop_writes_trigger + 36,
    //           cfd->GetLatestMutableCFOptions()->write_buffer_size * 2);
    //       break;

    //     case QueueMonitor::QueueState::QueueLoadState::Underload:
    //       db_options_.env->SetBackgroundThreads(
    //           (db_options_.env->GetBackgroundThreads(Env::Priority::HIGH) > 4)
    //               ? 5
    //               : db_options_.env->GetBackgroundThreads(Env::Priority::HIGH) +
    //                     1,
    //           Env::Priority::HIGH);
    //       db_options_.env->SetBackgroundThreads(
    //           (db_options_.env->GetBackgroundThreads(Env::Priority::LOW) < 2)
    //               ? 1
    //               : db_options_.env->GetBackgroundThreads(Env::Priority::LOW) /
    //                     2,
    //           Env::Priority::LOW);
    //       write_buffer_size =
    //           (2 * cfd->GetLatestMutableCFOptions()->write_buffer_size >
    //            512 << 20)
    //               ? 512 << 20
    //               : 2 * cfd->GetLatestMutableCFOptions()->write_buffer_size;
    //       cfd->SetWriteStallParameter(
    //           cfd->GetLatestMutableCFOptions()
    //                   ->level0_file_num_compaction_trigger -
    //               4,
    //           cfd->GetLatestMutableCFOptions()->level0_slowdown_writes_trigger -
    //               20,
    //           cfd->GetLatestMutableCFOptions()->level0_stop_writes_trigger - 36,
    //           cfd->GetLatestMutableCFOptions()->write_buffer_size / 2);
    //       break;
    //     default:
    //       break;
    //   }
    // }
    // cfd->SetWriteStallParameter(12, 60, 108, 128 << 20);
    // db_options_.env->IncBackgroundThreadsIfNeeded(8, Env::Priority::LOW);
    // db_options_.env->SetBackgroundThreads(4, Env::Priority::LOW);
    // fout << cfd->queue_moniter.DeltaTime(cfd->queue_moniter.l0_queue.service_,
    //                                      current_)
    //      << ',' << compact_->compaction->num_input_files(0) << ','
    //      << compact_->compaction->num_input_files(1) << ','
    //      << cfd->queue_moniter.l0_queue.arrive_rate_ << ','
    //      << cfd->queue_moniter.l0_queue.service_rate_ << ','
    //      << cfd->GetLatestMutableCFOptions()->level0_slowdown_writes_trigger
    //      << ',' << cfd->GetLatestMutableCFOptions()->write_buffer_size << ','
    //      << db_options_.env->GetBackgroundThreads(Env::Priority::HIGH) << ','
    //      << db_options_.env->GetBackgroundThreads(Env::Priority::LOW)
    //      << std::endl;
    // fout.close();
    // cfd->queue_moniter.l0_queue.service_ = current_;
    // cfd->queue_moniter.l0_queue.cal_start_ = false;
    // cfd->queue_moniter.l0_queue.cal_done_ = false;
    // cfd->queue_moniter.l0_arrive_ = 0;

    // cfd->queue_moniter.PrintTime(cfd->queue_moniter.start_time, current_,
    // out);

    // fout << cfd->queue_moniter.DeltaTime(cfd->queue_moniter.start_time,
    //                                      current_)
    //      << "\t" << cfd->current()->storage_info()->NumLevelFiles(0) << "\t"
    //      << double(cfd->current()
    //                    ->storage_info()
    //                    ->estimated_compaction_needed_bytes()) /
    //             1024 / 1024 / 1024
    //      << std::endl;
    // fout.close();
  }

  if (cfd->global_index->global_index_exists_ 
      && !cfd->global_index->EditEmpty()) {
    ReadOptions read_options;
    cfd->global_index->EditFiles(read_options);
  }

  if (!versions_->io_status().ok()) {
    io_status_ = versions_->io_status();
  }

  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = compaction_stats_;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  const uint64_t bytes_read_non_output_and_blob =
      stats.bytes_read_non_output_levels + stats.bytes_read_blob;
  const uint64_t bytes_read_all =
      stats.bytes_read_output_level + bytes_read_non_output_and_blob;
  const uint64_t bytes_written_all =
      stats.bytes_written + stats.bytes_written_blob;

  if (bytes_read_non_output_and_blob > 0) {
    read_write_amp = (bytes_written_all + bytes_read_all) /
                     static_cast<double>(bytes_read_non_output_and_blob);
    write_amp =
        bytes_written_all / static_cast<double>(bytes_read_non_output_and_blob);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec = bytes_read_all / static_cast<double>(stats.micros);
    bytes_written_per_sec =
        bytes_written_all / static_cast<double>(stats.micros);
  }

  const std::string& column_family_name = cfd->GetName();

  constexpr double kMB = 1048576.0;

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) out(%d +%d blob) "
      "MB in(%.1f, %.1f +%.1f blob) out(%.1f +%.1f blob), "
      "read-write-amplify(%.1f) write-amplify(%.1f) %s, records in: %" PRIu64
      ", records dropped: %" PRIu64 " output_compression: %s\n",
      column_family_name.c_str(), vstorage->LevelSummary(&tmp),
      bytes_read_per_sec, bytes_written_per_sec,
      compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level, stats.num_output_files,
      stats.num_output_files_blob, stats.bytes_read_non_output_levels / kMB,
      stats.bytes_read_output_level / kMB, stats.bytes_read_blob / kMB,
      stats.bytes_written / kMB, stats.bytes_written_blob / kMB, read_write_amp,
      write_amp, status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records,
      CompressionTypeToString(compact_->compaction->output_compression())
          .c_str());

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64
                     "\n",
                     column_family_name.c_str(), blob_files.begin()->first,
                     blob_files.rbegin()->first);
  }

  UpdateCompactionJobStats(stats);

  auto stream = event_logger_->LogToBuffer(log_buffer_);
  stream << "job" << job_id_ << "event"
         << "compaction_finished"
         << "compaction_time_micros" << stats.micros
         << "compaction_time_cpu_micros" << stats.cpu_micros << "output_level"
         << compact_->compaction->output_level() << "num_output_files"
         << compact_->num_output_files << "total_output_size"
         << compact_->total_bytes;

  if (compact_->num_blob_output_files > 0) {
    stream << "num_blob_output_files" << compact_->num_blob_output_files
           << "total_blob_output_size" << compact_->total_blob_bytes;
  }

  stream << "num_input_records" << stats.num_input_records
         << "num_output_records" << compact_->num_output_records
         << "num_subcompactions" << compact_->sub_compact_states.size()
         << "output_compression"
         << CompressionTypeToString(compact_->compaction->output_compression());

  stream << "num_single_delete_mismatches"
         << compaction_job_stats_->num_single_del_mismatch;
  stream << "num_single_delete_fallthrough"
         << compaction_job_stats_->num_single_del_fallthru;

  if (measure_io_stats_) {
    stream << "file_write_nanos" << compaction_job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos"
           << compaction_job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << compaction_job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << compaction_job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  if (!blob_files.empty()) {
    stream << "blob_file_head" << blob_files.begin()->first;
    stream << "blob_file_tail" << blob_files.rbegin()->first;
  }

  CleanupCompaction();
  return status;
}

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  assert(sub_compact);
  assert(sub_compact->compaction);

  uint64_t prev_cpu_micros = db_options_.clock->CPUNanos() / 1000;

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();

  // Create compaction filter and fail the compaction if
  // IgnoreSnapshots() = false because it is not supported anymore
  const CompactionFilter* compaction_filter =
      cfd->ioptions()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr) {
    compaction_filter_from_factory =
        sub_compact->compaction->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }
  if (compaction_filter != nullptr && !compaction_filter->IgnoreSnapshots()) {
    sub_compact->status = Status::NotSupported(
        "CompactionFilter::IgnoreSnapshots() = false is not supported "
        "anymore.");
    return;
  }

  CompactionRangeDelAggregator range_del_agg(&cfd->internal_comparator(),
                                             existing_snapshots_);
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  // Compaction iterators shouldn't be confined to a single prefix.
  // Compactions use Seek() for
  // (a) concurrent compactions,
  // (b) CompactionFilter::Decision::kRemoveAndSkipUntil.
  read_options.total_order_seek = true;

  // Although the v2 aggregator is what the level iterator(s) know about,
  // the AddTombstones calls will be propagated down to the v1 aggregator.
  std::unique_ptr<InternalIterator> input(
      versions_->MakeInputIterator(read_options, sub_compact->compaction,
                                   &range_del_agg, file_options_for_read_));

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  const uint64_t kRecordStatsEvery = 1000;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  MergeHelper merge(
      env_, cfd->user_comparator(), cfd->ioptions()->merge_operator,
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      snapshot_checker_, compact_->compaction->level(),
      db_options_.statistics.get());

  const MutableCFOptions* mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();
  assert(mutable_cf_options);

  std::vector<std::string> blob_file_paths;

  std::unique_ptr<BlobFileBuilder> blob_file_builder(
      mutable_cf_options->enable_blob_files
          ? new BlobFileBuilder(versions_, fs_.get(),
                                sub_compact->compaction->immutable_cf_options(),
                                mutable_cf_options, &file_options_, job_id_,
                                cfd->GetID(), cfd->GetName(),
                                Env::IOPriority::IO_LOW, write_hint_,
                                io_tracer_, blob_callback_, &blob_file_paths,
                                &sub_compact->blob_file_additions)
          : nullptr);

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::Run():PausingManualCompaction:1",
      reinterpret_cast<void*>(
          const_cast<std::atomic<int>*>(manual_compaction_paused_)));

  Slice* start = sub_compact->start;
  Slice* end = sub_compact->end;
  if (start != nullptr) {
    IterKey start_iter;
    start_iter.SetInternalKey(*start, kMaxSequenceNumber, kValueTypeForSeek);
    input->Seek(start_iter.GetInternalKey());
  } else {
    input->SeekToFirst();
  }

  Status status;
  const std::string* const full_history_ts_low =
      full_history_ts_low_.empty() ? nullptr : &full_history_ts_low_;
  sub_compact->c_iter.reset(new CompactionIterator(
      input.get(), cfd->user_comparator(), &merge, versions_->LastSequence(),
      &existing_snapshots_, earliest_write_conflict_snapshot_,
      snapshot_checker_, env_, ShouldReportDetailedTime(env_, stats_),
      /*expect_valid_internal_key=*/true, &range_del_agg,
      blob_file_builder.get(), db_options_.allow_data_in_errors,
      sub_compact->compaction, compaction_filter, shutting_down_,
      preserve_deletes_seqnum_, manual_compaction_paused_, db_options_.info_log,
      full_history_ts_low));
  auto c_iter = sub_compact->c_iter.get();
  c_iter->SeekToFirst();
  if (c_iter->Valid() && sub_compact->compaction->output_level() != 0) {
    // ShouldStopBefore() maintains state based on keys processed so far. The
    // compaction loop always calls it on the "next" key, thus won't tell it the
    // first key. So we do that here.
    sub_compact->ShouldStopBefore(c_iter->key(),
                                  sub_compact->current_output_file_size);
  }
  const auto& c_iter_stats = c_iter->iter_stats();

  std::unique_ptr<SstPartitioner> partitioner =
      sub_compact->compaction->output_level() == 0
          ? nullptr
          : sub_compact->compaction->CreateSstPartitioner();
  std::string last_key_for_partitioner;

  while (status.ok() && !cfd->IsDropped() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.
    const Slice& key = c_iter->key();
    const Slice& value = c_iter->value();

    // If an end key (exclusive) is specified, check if the current key is
    // >= than it and exit if it is because the iterator is out of its range
    if (end != nullptr &&
        cfd->user_comparator()->Compare(c_iter->user_key(), *end) >= 0) {
      break;
    }
    if (c_iter_stats.num_input_records % kRecordStatsEvery ==
        kRecordStatsEvery - 1) {
      RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
      c_iter->ResetRecordCounts();
      RecordCompactionIOStats();
    }

    // Open output file if necessary
    if (sub_compact->builder == nullptr) {
      status = OpenCompactionOutputFile(sub_compact);
      if (!status.ok()) {
        break;
      }
    }
    status = sub_compact->AddToBuilder(key, value);
    if (!status.ok()) {
      break;
    }

    sub_compact->current_output_file_size =
        sub_compact->builder->EstimatedFileSize();
    const ParsedInternalKey& ikey = c_iter->ikey();
    sub_compact->current_output()->meta.UpdateBoundaries(
        key, value, ikey.sequence, ikey.type);
    sub_compact->num_output_records++;

    // Close output file if it is big enough. Two possibilities determine it's
    // time to close it: (1) the current key should be this file's last key, (2)
    // the next key should not be in this file.
    //
    // TODO(aekmekji): determine if file should be closed earlier than this
    // during subcompactions (i.e. if output size, estimated by input size, is
    // going to be 1.2MB and max_output_file_size = 1MB, prefer to have 0.6MB
    // and 0.6MB instead of 1MB and 0.2MB)
    bool output_file_ended = false;
    if (sub_compact->compaction->output_level() != 0 &&
        sub_compact->current_output_file_size >=
            sub_compact->compaction->max_output_file_size()) {
      // (1) this key terminates the file. For historical reasons, the iterator
      // status before advancing will be given to FinishCompactionOutputFile().
      output_file_ended = true;
    }
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::Run():PausingManualCompaction:2",
        reinterpret_cast<void*>(
            const_cast<std::atomic<int>*>(manual_compaction_paused_)));
    if (partitioner.get()) {
      last_key_for_partitioner.assign(c_iter->user_key().data_,
                                      c_iter->user_key().size_);
    }
    c_iter->Next();
    if (c_iter->status().IsManualCompactionPaused()) {
      break;
    }
    if (!output_file_ended && c_iter->Valid()) {
      if (((partitioner.get() &&
            partitioner->ShouldPartition(PartitionerRequest(
                last_key_for_partitioner, c_iter->user_key(),
                sub_compact->current_output_file_size)) == kRequired) ||
           (sub_compact->compaction->output_level() != 0 &&
            sub_compact->ShouldStopBefore(
                c_iter->key(), sub_compact->current_output_file_size))) &&
          sub_compact->builder != nullptr) {
        // (2) this key belongs to the next file. For historical reasons, the
        // iterator status after advancing will be given to
        // FinishCompactionOutputFile().
        output_file_ended = true;
      }
    }
    if (output_file_ended) {
      const Slice* next_key = nullptr;
      if (c_iter->Valid()) {
        next_key = &c_iter->key();
      }
      CompactionIterationStats range_del_out_stats;
      status = FinishCompactionOutputFile(input->status(), sub_compact,
                                          &range_del_agg, &range_del_out_stats,
                                          next_key);
      RecordDroppedKeys(range_del_out_stats,
                        &sub_compact->compaction_job_stats);
    }
  }

  sub_compact->compaction_job_stats.num_blobs_read =
      c_iter_stats.num_blobs_read;
  sub_compact->compaction_job_stats.total_blob_bytes_read =
      c_iter_stats.total_blob_bytes_read;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);
  RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
  RecordCompactionIOStats();

  if (status.ok() && cfd->IsDropped()) {
    status =
        Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_relaxed)) {
    status = Status::ShutdownInProgress("Database shutdown");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      (manual_compaction_paused_ &&
       manual_compaction_paused_->load(std::memory_order_relaxed) > 0)) {
    status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (status.ok() && sub_compact->builder == nullptr &&
      sub_compact->outputs.size() == 0 && !range_del_agg.IsEmpty()) {
    // handle subcompaction containing only range deletions
    status = OpenCompactionOutputFile(sub_compact);
  }

  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output file.
  if (sub_compact->builder != nullptr) {
    CompactionIterationStats range_del_out_stats;
    Status s = FinishCompactionOutputFile(status, sub_compact, &range_del_agg,
                                          &range_del_out_stats);
    if (!s.ok() && status.ok()) {
      status = s;
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
  }

  if (blob_file_builder) {
    if (status.ok()) {
      status = blob_file_builder->Finish();
    } else {
      blob_file_builder->Abandon();
    }
    blob_file_builder.reset();
  }

  sub_compact->compaction_job_stats.cpu_micros =
      db_options_.clock->CPUNanos() / 1000 - prev_cpu_micros;

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    sub_compact->compaction_job_stats.cpu_micros -=
        (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos +
         IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos) /
        1000;
    if (prev_perf_level != PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) {
      SetPerfLevel(prev_perf_level);
    }
  }
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  if (!status.ok()) {
    if (sub_compact->c_iter) {
      sub_compact->c_iter->status().PermitUncheckedError();
    }
    if (input) {
      input->status().PermitUncheckedError();
    }
  }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  sub_compact->c_iter.reset();
  input.reset();
  sub_compact->status = status;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    CompactionRangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const Slice* next_table_min_key /* = nullptr */) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(sub_compact->outfile);
  assert(sub_compact->builder != nullptr);
  assert(sub_compact->current_output() != nullptr);

  uint64_t output_number = sub_compact->current_output()->meta.fd.GetNumber();
  assert(output_number != 0);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  const Comparator* ucmp = cfd->user_comparator();
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  // Check for iterator errors
  Status s = input_status;
  auto meta = &sub_compact->current_output()->meta;
  assert(meta != nullptr);
  if (s.ok()) {
    Slice lower_bound_guard, upper_bound_guard;
    std::string smallest_user_key;
    const Slice *lower_bound, *upper_bound;
    bool lower_bound_from_sub_compact = false;
    if (sub_compact->outputs.size() == 1) {
      // For the first output table, include range tombstones before the min key
      // but after the subcompaction boundary.
      lower_bound = sub_compact->start;
      lower_bound_from_sub_compact = true;
    } else if (meta->smallest.size() > 0) {
      // For subsequent output tables, only include range tombstones from min
      // key onwards since the previous file was extended to contain range
      // tombstones falling before min key.
      smallest_user_key = meta->smallest.user_key().ToString(false /*hex*/);
      lower_bound_guard = Slice(smallest_user_key);
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
    if (next_table_min_key != nullptr) {
      // This may be the last file in the subcompaction in some cases, so we
      // need to compare the end key of subcompaction with the next file start
      // key. When the end key is chosen by the subcompaction, we know that
      // it must be the biggest key in output file. Therefore, it is safe to
      // use the smaller key as the upper bound of the output file, to ensure
      // that there is no overlapping between different output files.
      upper_bound_guard = ExtractUserKey(*next_table_min_key);
      if (sub_compact->end != nullptr &&
          ucmp->Compare(upper_bound_guard, *sub_compact->end) >= 0) {
        upper_bound = sub_compact->end;
      } else {
        upper_bound = &upper_bound_guard;
      }
    } else {
      // This is the last file in the subcompaction, so extend until the
      // subcompaction ends.
      upper_bound = sub_compact->end;
    }
    auto earliest_snapshot = kMaxSequenceNumber;
    if (existing_snapshots_.size() > 0) {
      earliest_snapshot = existing_snapshots_[0];
    }
    bool has_overlapping_endpoints;
    if (upper_bound != nullptr && meta->largest.size() > 0) {
      has_overlapping_endpoints =
          ucmp->Compare(meta->largest.user_key(), *upper_bound) == 0;
    } else {
      has_overlapping_endpoints = false;
    }

    // The end key of the subcompaction must be bigger or equal to the upper
    // bound. If the end of subcompaction is null or the upper bound is null,
    // it means that this file is the last file in the compaction. So there
    // will be no overlapping between this file and others.
    assert(sub_compact->end == nullptr ||
           upper_bound == nullptr ||
           ucmp->Compare(*upper_bound , *sub_compact->end) <= 0);
    auto it = range_del_agg->NewIterator(lower_bound, upper_bound,
                                         has_overlapping_endpoints);
    // Position the range tombstone output iterator. There may be tombstone
    // fragments that are entirely out of range, so make sure that we do not
    // include those.
    if (lower_bound != nullptr) {
      it->Seek(*lower_bound);
    } else {
      it->SeekToFirst();
    }
    TEST_SYNC_POINT("CompactionJob::FinishCompactionOutputFile1");
    for (; it->Valid(); it->Next()) {
      auto tombstone = it->Tombstone();
      if (upper_bound != nullptr) {
        int cmp = ucmp->Compare(*upper_bound, tombstone.start_key_);
        if ((has_overlapping_endpoints && cmp < 0) ||
            (!has_overlapping_endpoints && cmp <= 0)) {
          // Tombstones starting after upper_bound only need to be included in
          // the next table. If the current SST ends before upper_bound, i.e.,
          // `has_overlapping_endpoints == false`, we can also skip over range
          // tombstones that start exactly at upper_bound. Such range tombstones
          // will be included in the next file and are not relevant to the point
          // keys or endpoints of the current file.
          break;
        }
      }

      if (bottommost_level_ && tombstone.seq_ <= earliest_snapshot) {
        // TODO(andrewkr): tombstones that span multiple output files are
        // counted for each compaction output file, so lots of double counting.
        range_del_out_stats->num_range_del_drop_obsolete++;
        range_del_out_stats->num_record_drop_obsolete++;
        continue;
      }

      auto kv = tombstone.Serialize();
      assert(lower_bound == nullptr ||
             ucmp->Compare(*lower_bound, kv.second) < 0);
      // Range tombstone is not supported by output validator yet.
      sub_compact->builder->Add(kv.first.Encode(), kv.second);
      InternalKey smallest_candidate = std::move(kv.first);
      if (lower_bound != nullptr &&
          ucmp->Compare(smallest_candidate.user_key(), *lower_bound) <= 0) {
        // Pretend the smallest key has the same user key as lower_bound
        // (the max key in the previous table or subcompaction) in order for
        // files to appear key-space partitioned.
        //
        // When lower_bound is chosen by a subcompaction, we know that
        // subcompactions over smaller keys cannot contain any keys at
        // lower_bound. We also know that smaller subcompactions exist, because
        // otherwise the subcompaction woud be unbounded on the left. As a
        // result, we know that no other files on the output level will contain
        // actual keys at lower_bound (an output file may have a largest key of
        // lower_bound@kMaxSequenceNumber, but this only indicates a large range
        // tombstone was truncated). Therefore, it is safe to use the
        // tombstone's sequence number, to ensure that keys at lower_bound at
        // lower levels are covered by truncated tombstones.
        //
        // If lower_bound was chosen by the smallest data key in the file,
        // choose lowest seqnum so this file's smallest internal key comes after
        // the previous file's largest. The fake seqnum is OK because the read
        // path's file-picking code only considers user key.
        smallest_candidate = InternalKey(
            *lower_bound, lower_bound_from_sub_compact ? tombstone.seq_ : 0,
            kTypeRangeDeletion);
      }
      InternalKey largest_candidate = tombstone.SerializeEndKey();
      if (upper_bound != nullptr &&
          ucmp->Compare(*upper_bound, largest_candidate.user_key()) <= 0) {
        // Pretend the largest key has the same user key as upper_bound (the
        // min key in the following table or subcompaction) in order for files
        // to appear key-space partitioned.
        //
        // Choose highest seqnum so this file's largest internal key comes
        // before the next file's/subcompaction's smallest. The fake seqnum is
        // OK because the read path's file-picking code only considers the user
        // key portion.
        //
        // Note Seek() also creates InternalKey with (user_key,
        // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
        // kTypeRangeDeletion (0xF), so the range tombstone comes before the
        // Seek() key in InternalKey's ordering. So Seek() will look in the
        // next file for the user key.
        largest_candidate =
            InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
      }
#ifndef NDEBUG
      SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
      if (meta->smallest.size() > 0) {
        smallest_ikey_seqnum = GetInternalKeySeqno(meta->smallest.Encode());
      }
#endif
      meta->UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                     tombstone.seq_,
                                     cfd->internal_comparator());

      // The smallest key in a file is used for range tombstone truncation, so
      // it cannot have a seqnum of 0 (unless the smallest data key in a file
      // has a seqnum of 0). Otherwise, the truncated tombstone may expose
      // deleted keys at lower levels.
      assert(smallest_ikey_seqnum == 0 ||
             ExtractInternalKeyFooter(meta->smallest.Encode()) !=
                 PackSequenceAndType(0, kTypeRangeDeletion));
    }
  }
  const uint64_t current_entries = sub_compact->builder->NumEntries();
  if (s.ok()) {
    s = sub_compact->builder->Finish();
  } else {
    sub_compact->builder->Abandon();
  }
  IOStatus io_s = sub_compact->builder->io_status();
  if (s.ok()) {
    s = io_s;
  }
  const uint64_t current_bytes = sub_compact->builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
    meta->marked_for_compaction = sub_compact->builder->NeedCompact();
  }
  sub_compact->current_output()->finished = true;
  sub_compact->total_bytes += current_bytes;

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(db_options_.clock, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    io_s = sub_compact->outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok() && io_s.ok()) {
    io_s = sub_compact->outfile->Close();
  }
  if (s.ok() && io_s.ok()) {
    // Add the checksum information to file metadata.
    meta->file_checksum = sub_compact->outfile->GetFileChecksum();
    meta->file_checksum_func_name =
        sub_compact->outfile->GetFileChecksumFuncName();
    file_checksum = meta->file_checksum;
    file_checksum_func_name = meta->file_checksum_func_name;
  }
  if (s.ok()) {
    s = io_s;
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the
    // "normal" status, it does not also need to be checked
    sub_compact->io_status.PermitUncheckedError();
  }
  sub_compact->outfile.reset();

  TableProperties tp;
  if (s.ok()) {
    tp = sub_compact->builder->GetTableProperties();
  }

  if (s.ok() && current_entries == 0 && tp.num_range_deletions == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());

    // TODO(AR) it is not clear if there are any larger implications if
    // DeleteFile fails here
    Status ds = env_->DeleteFile(fname);
    if (!ds.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] Unable to remove SST file for table #%" PRIu64
          " at bottom level%s",
          cfd->GetName().c_str(), job_id_, output_number,
          meta->marked_for_compaction ? " (need compaction)" : "");
    }

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    assert(!sub_compact->outputs.empty());
    sub_compact->outputs.pop_back();
    meta = nullptr;
  }

  if (s.ok() && (current_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    sub_compact->current_output()->table_properties =
        std::make_shared<TableProperties>(tp);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, output_number,
                   current_entries, current_bytes,
                   meta->marked_for_compaction ? " (need compaction)" : "");
  }
  std::string fname;
  FileDescriptor output_fd;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
  if (meta != nullptr) {
    fname =
        TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());
    output_fd = meta->fd;
    oldest_blob_file_number = meta->oldest_blob_file_number;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, oldest_blob_file_number, tp,
      TableFileCreationReason::kCompaction, s, file_checksum,
      file_checksum_func_name);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    Status add_s = sfm->OnAddFile(fname);
    if (!add_s.ok() && s.ok()) {
      s = add_s;
    }
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }
#endif

  sub_compact->builder.reset();
  sub_compact->current_output_file_size = 0;
  return s;
}

Status CompactionJob::InstallCompactionResults(
    const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  db_mutex_->AssertHeld();

  auto* compaction = compact_->compaction;
  assert(compaction);

  // paranoia: verify that the files that we started with
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact_.
  if (!versions_->VerifyCompactionFileConsistency(compaction)) {
    Compaction::InputLevelSummaryBuffer inputs_summary;

    ROCKS_LOG_ERROR(db_options_.info_log, "[%s] [JOB %d] Compaction %s aborted",
                    compaction->column_family_data()->GetName().c_str(),
                    job_id_, compaction->InputLevelSummary(&inputs_summary));
    return Status::Corruption("Compaction input files inconsistent");
  }

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Compacted %s => %" PRIu64 " bytes",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   compaction->InputLevelSummary(&inputs_summary),
                   compact_->total_bytes + compact_->total_blob_bytes);
  }

  VersionEdit* const edit = compaction->edit();
  assert(edit);

  // Add compaction inputs
  compaction->AddInputDeletions(edit);
  ReadOptions read_options;

  for (const auto& sub_compact : compact_->sub_compact_states) {
    for (const auto& out : sub_compact.outputs) {
      edit->AddFile(compaction->output_level(), out.meta);
      if (compaction->column_family_data()->global_index->global_index_exists_) {
        compaction->column_family_data()->global_index->add_files_.emplace(
            out.meta.fd.GetNumber(),
            GlobalIndex::EditFile(compaction->output_level(), out.meta.smallest,
                                  out.meta.largest));
      }
    }

    for (const auto& blob : sub_compact.blob_file_additions) {
      edit->AddBlobFile(blob);
    }
  }

  return versions_->LogAndApply(compaction->column_family_data(),
                                mutable_cf_options, edit, db_mutex_,
                                db_directory_);
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  CompactionReason compaction_reason =
      compact_->compaction->compaction_reason();
  if (compaction_reason == CompactionReason::kFilesMarkedForCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_MARKED, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_MARKED, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kPeriodicCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_PERIODIC, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_PERIODIC, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kTtl) {
    RecordTick(stats_, COMPACT_READ_BYTES_TTL, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_TTL, IOSTATS(bytes_written));
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(
    SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  assert(sub_compact->builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname =
      TableFileName(sub_compact->compaction->immutable_cf_options()->cf_paths,
                    file_number, sub_compact->compaction->output_path_id());
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, job_id_,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<FSWritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = file_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif
  Status s;
  IOStatus io_s =
      NewWritableFile(fs_.get(), fname, &writable_file, file_options_);
  s = io_s;
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the io_s that is checked below as s,
    // it does not also need to be checked.
    sub_compact->io_status.PermitUncheckedError();
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    return s;
  }

  // Try to figure out the output file's oldest ancester time.
  int64_t temp_current_time = 0;
  auto get_time_status = db_options_.clock->GetCurrentTime(&temp_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!get_time_status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get current time. Status: %s",
                   get_time_status.ToString().c_str());
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);
  uint64_t oldest_ancester_time =
      sub_compact->compaction->MinInputFileOldestAncesterTime();
  if (oldest_ancester_time == port::kMaxUint64) {
    oldest_ancester_time = current_time;
  }

  // Initialize a SubcompactionState::Output and add it to sub_compact->outputs
  {
    FileMetaData meta;
    meta.fd = FileDescriptor(file_number,
                             sub_compact->compaction->output_path_id(), 0);
    meta.oldest_ancester_time = oldest_ancester_time;
    meta.file_creation_time = current_time;
    sub_compact->outputs.emplace_back(
        std::move(meta), cfd->internal_comparator(),
        /*enable_order_check=*/
        sub_compact->compaction->mutable_cf_options()
            ->check_flush_compaction_key_order,
        /*enable_hash=*/paranoid_file_checks_);
  }

  writable_file->SetIOPriority(Env::IOPriority::IO_LOW);
  writable_file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = db_options_.checksum_handoff_file_types;
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  const auto& listeners =
      sub_compact->compaction->immutable_cf_options()->listeners;
  sub_compact->outfile.reset(new WritableFileWriter(
      std::move(writable_file), fname, file_options_, db_options_.clock,
      io_tracer_, db_options_.statistics.get(), listeners,
      db_options_.file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kTableFile)));

  // If the Column family flag is to only optimize filters for hits,
  // we can skip creating filters if this is the bottommost_level where
  // data is going to be found
  bool skip_filters =
      cfd->ioptions()->optimize_filters_for_hits && bottommost_level_;

  sub_compact->builder.reset(NewTableBuilder(
      *cfd->ioptions(), *(sub_compact->compaction->mutable_cf_options()),
      cfd->internal_comparator(), cfd->int_tbl_prop_collector_factories(),
      cfd->GetID(), cfd->GetName(), sub_compact->outfile.get(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(),
      sub_compact->compaction->output_level(), skip_filters,
      oldest_ancester_time, 0 /* oldest_key_time */,
      sub_compact->compaction->max_output_file_size(), current_time, db_id_,
      db_session_id_));
  LogFlush(db_options_.info_log);
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    const auto& sub_status = sub_compact.status;

    if (sub_compact.builder != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      sub_compact.builder->Abandon();
      sub_compact.builder.reset();
    } else {
      assert(!sub_status.ok() || sub_compact.outfile == nullptr);
    }
    for (const auto& out : sub_compact.outputs) {
      // If this file was inserted into the table cache then remove
      // them here because this compaction was not committed.
      if (!sub_status.ok()) {
        TableCache::Evict(table_cache_.get(), out.meta.fd.GetNumber());
      }
    }
    // TODO: sub_compact.io_status is not checked like status. Not sure if thats
    // intentional. So ignoring the io_status as of now.
    sub_compact.io_status.PermitUncheckedError();
  }
  delete compact_;
  compact_ = nullptr;
}

#ifndef ROCKSDB_LITE
namespace {
void CopyPrefix(const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

#endif  // !ROCKSDB_LITE

void CompactionJob::UpdateCompactionStats() {
  assert(compact_);

  Compaction* compaction = compact_->compaction;
  compaction_stats_.num_input_files_in_non_output_levels = 0;
  compaction_stats_.num_input_files_in_output_level = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    if (compaction->level(input_level) != compaction->output_level()) {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_non_output_levels,
          &compaction_stats_.bytes_read_non_output_levels, input_level);
    } else {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_output_level,
          &compaction_stats_.bytes_read_output_level, input_level);
    }
  }

  assert(compaction_job_stats_);
  compaction_stats_.bytes_read_blob =
      compaction_job_stats_->total_blob_bytes_read;

  compaction_stats_.num_output_files =
      static_cast<int>(compact_->num_output_files);
  compaction_stats_.num_output_files_blob =
      static_cast<int>(compact_->num_blob_output_files);
  compaction_stats_.bytes_written = compact_->total_bytes;
  compaction_stats_.bytes_written_blob = compact_->total_blob_bytes;

  if (compaction_stats_.num_input_records > compact_->num_output_records) {
    compaction_stats_.num_dropped_records =
        compaction_stats_.num_input_records - compact_->num_output_records;
  }
}

void CompactionJob::UpdateCompactionInputStatsHelper(int* num_files,
                                                     uint64_t* bytes_read,
                                                     int input_level) {
  const Compaction* compaction = compact_->compaction;
  auto num_input_files = compaction->num_input_files(input_level);
  *num_files += static_cast<int>(num_input_files);

  for (size_t i = 0; i < num_input_files; ++i) {
    const auto* file_meta = compaction->input(input_level, i);
    *bytes_read += file_meta->fd.GetFileSize();
    compaction_stats_.num_input_records +=
        static_cast<uint64_t>(file_meta->num_entries);
  }
}

void CompactionJob::UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const {
#ifndef ROCKSDB_LITE
  compaction_job_stats_->elapsed_micros = stats.micros;

  // input information
  compaction_job_stats_->total_input_bytes =
      stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
  compaction_job_stats_->num_input_records = stats.num_input_records;
  compaction_job_stats_->num_input_files =
      stats.num_input_files_in_non_output_levels +
      stats.num_input_files_in_output_level;
  compaction_job_stats_->num_input_files_at_output_level =
      stats.num_input_files_in_output_level;

  // output information
  compaction_job_stats_->total_output_bytes = stats.bytes_written;
  compaction_job_stats_->total_output_bytes_blob = stats.bytes_written_blob;
  compaction_job_stats_->num_output_records = compact_->num_output_records;
  compaction_job_stats_->num_output_files = stats.num_output_files;
  compaction_job_stats_->num_output_files_blob = stats.num_output_files_blob;

  if (stats.num_output_files > 0) {
    CopyPrefix(compact_->SmallestUserKey(),
               CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->smallest_output_key_prefix);
    CopyPrefix(compact_->LargestUserKey(), CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->largest_output_key_prefix);
  }
#else
  (void)stats;
#endif  // !ROCKSDB_LITE
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();

  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] Compaction start summary: %s\n",
                   cfd->GetName().c_str(), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event"
           << "compaction_started"
           << "compaction_reason"
           << GetCompactionReasonString(compaction->compaction_reason());
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + ToString(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize();
  }
}

}  // namespace ROCKSDB_NAMESPACE
