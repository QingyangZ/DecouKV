//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/blob/blob_file_builder.h"
#include "db/compaction/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

class TableFactory;

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts, int level,
    const bool skip_filters, const uint64_t creation_time,
    const uint64_t oldest_key_time, const uint64_t target_file_size,
    const uint64_t file_creation_time, const std::string& db_id,
    const std::string& db_session_id) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  return ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, skip_filters, column_family_name,
                          level, creation_time, oldest_key_time,
                          target_file_size, file_creation_time, db_id,
                          db_session_id),
      column_family_id, file);
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions,
    const ImmutableDBOptions& db_options, const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const FileOptions& file_options,
    TableCache* table_cache, InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, std::vector<BlobFileAddition>* blob_file_additions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    IOStatus* io_status, const std::shared_ptr<IOTracer>& io_tracer,
    EventLogger* event_logger, int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, int level, const uint64_t creation_time,
    const uint64_t oldest_key_time, Env::WriteLifeTimeHint write_hint,
    const uint64_t file_creation_time, const std::string& db_id,
    const std::string& db_session_id, const std::string* full_history_ts_low,
    BlobFileCompletionCallback* blob_callback, KeyTable* next_table) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  OutputValidator output_validator(
      internal_comparator,
      /*enable_order_check=*/
      mutable_cf_options.check_flush_compaction_key_order,
      /*enable_hash=*/paranoid_file_checks);
  Status s;
  meta->fd.file_size = 0;
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparator, snapshots));
  for (auto& range_del_iter : range_del_iters) {
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      ioptions.listeners, dbname, column_family_name, fname, job_id, reason);
#endif  // !ROCKSDB_LITE
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);

  TableProperties tp;
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<FSWritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = file_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      IOStatus io_s = NewWritableFile(fs, fname, &file, file_options);
      assert(s.ok());
      s = io_s;
      if (io_status->ok()) {
        *io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname, column_family_name, fname,
            job_id, meta->fd, kInvalidBlobFileNumber, tp, reason, s,
            file_checksum, file_checksum_func_name);
        return s;
      }
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, io_tracer,
          ioptions.statistics, ioptions.listeners,
          ioptions.file_checksum_gen_factory,
          tmp_set.Contains(FileType::kTableFile)));

      builder = NewTableBuilder(
          ioptions, mutable_cf_options, internal_comparator,
          int_tbl_prop_collector_factories, column_family_id,
          column_family_name, file_writer.get(), compression, compression_opts,
          level, false /* skip_filters */, creation_time, oldest_key_time,
          0 /*target_file_size*/, file_creation_time, db_id, db_session_id);
    }

    MergeHelper merge(env, internal_comparator.user_comparator(),
                      ioptions.merge_operator, nullptr, ioptions.info_log,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(),
                      snapshot_checker);

    std::unique_ptr<BlobFileBuilder> blob_file_builder(
        (mutable_cf_options.enable_blob_files && blob_file_additions)
            ? new BlobFileBuilder(versions, fs, &ioptions, &mutable_cf_options,
                                  &file_options, job_id, column_family_id,
                                  column_family_name, io_priority, write_hint,
                                  io_tracer, blob_callback, &blob_file_paths,
                                  blob_file_additions)
            : nullptr);

    CompactionIterator c_iter(
        iter, internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
        &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.statistics),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        blob_file_builder.get(), ioptions.allow_data_in_errors,
        /*compaction=*/nullptr,
        /*compaction_filter=*/nullptr, /*shutting_down=*/nullptr,
        /*preserve_deletes_seqnum=*/0, /*manual_compaction_paused=*/nullptr,
        db_options.info_log, full_history_ts_low);

    InternalIterator* n_iter;
    InternalIterator* prev_iter;
    Arena arena;
    if (next_table != nullptr && !next_table->IsEmpty()) {
      n_iter = next_table->mems_[0]->NewIterator(ReadOptions(), &arena);
      prev_iter = next_table->mems_[0]->NewIterator(ReadOptions(), &arena);
      n_iter->SeekToFirst();
      prev_iter->SeekToHead();
      iter->SeekToHead();
      // Slice* new_value = new Slice();
      iter->ReplaceValue(/**new_value, */prev_iter->GetNode());
      iter->SeekToFirst();
    }
    c_iter.SeekToFirst();
    for (; c_iter.Valid(); c_iter.Next()) {
      // const Slice& key = c_iter.key();
      // const Slice& value = c_iter.value();
      // const ParsedInternalKey& ikey = c_iter.ikey();

      ParsedInternalKey ikey = *(const_cast<ParsedInternalKey*>(&c_iter.ikey()));
      ikey.type = kTypeValue;
      std::string str;
      AppendInternalKey(&str, ikey);
      Slice key(str);
      // tht update
      size_t key_size = ikey.user_key.size();
      Slice blob_index_entry = c_iter.value();
      BlobIndex blob_index;
      s = blob_index.DecodeFrom(blob_index_entry);
      if (!s.ok()) {
        return s;
      }
      CompressionType compression_type = kNoCompression;
      auto blob_file = std::make_shared<blob_db::BlobFile>(
          nullptr, db_options.blob_path, blob_index.file_number(),
          db_options.info_log.get());
      bool fresh_open = false;
      std::shared_ptr<RandomAccessFileReader> reader;
      s = blob_file->GetReader(env, file_options, &reader, &fresh_open);
      if (!s.ok()) {
        return s;
      }
      assert(blob_index.offset() >= key_size + sizeof(uint32_t));
      const uint64_t record_offset =
          blob_index.offset() - key_size - sizeof(uint32_t);
      const uint64_t record_size =
          sizeof(uint32_t) + key_size + blob_index.size();
      // Allocate the buffer. This is safe in C++11
      std::string buf;
      AlignedBuf aligned_buf;

      // A partial blob record contain checksum, key and value.
      Slice blob_record;
      {
        if (reader->use_direct_io()) {
          s = reader->Read(IOOptions(), record_offset,
                           static_cast<size_t>(record_size), &blob_record,
                           nullptr, &aligned_buf);
        } else {
          buf.reserve(static_cast<size_t>(record_size));
          s = reader->Read(IOOptions(), record_offset,
                           static_cast<size_t>(record_size), &blob_record,
                           &buf[0], nullptr);
        }
      }
      if (!s.ok()) {
        return s;
      }

      if (blob_record.size() != record_size) {
        return Status::Corruption("Failed to retrieve blob from blob index.");
      }
      Slice crc_slice(blob_record.data(), sizeof(uint32_t));
      Slice blob_value(blob_record.data() + sizeof(uint32_t) + key_size,
                       static_cast<size_t>(blob_index.size()));

      uint32_t crc_exp = 0;
      if (!GetFixed32(&crc_slice, &crc_exp)) {
        return Status::Corruption("Unable to decode checksum.");
      }

      uint32_t crc = crc32c::Value(blob_record.data() + sizeof(uint32_t),
                               blob_record.size() - sizeof(uint32_t));
      crc = crc32c::Mask(crc);  // Adjust for storage
      if (crc != crc_exp) {
        return Status::Corruption("Corruption. Blob CRC mismatch");
      }
      const Slice& value = blob_value;

      // Generate a rolling 64-bit hash of the key and values
      s = output_validator.Add(key, value);
      if (!s.ok()) {
        break;
      }
      // modified by zqy
      uint64_t offset;
      size_t restart;
      builder->Add(key, value, &offset, &restart);
      std::string value_location =
          std::to_string(offset) + " " + std::to_string(restart);
      // Slice* new_value = new Slice(value_location);

      void* next_node;
      if (next_table != nullptr && !next_table->IsEmpty() && n_iter != nullptr) {
        next_node = const_cast<void*>(next_table->AddPtrWithNode(ikey, &n_iter, &prev_iter));
        assert(next_node != nullptr);
        c_iter.ReplaceValue(/**new_value, */next_node);
      }

      meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        auto kv = tombstone.Serialize();
        builder->Add(kv.first.Encode(), kv.second);
        meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                       tombstone.seq_, internal_comparator);
      }
    }

    TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
    const bool empty = builder->IsEmpty();
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      s = builder->Finish();
    }
    if (io_status->ok()) {
      *io_status = builder->io_status();
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      tp = builder->GetTableProperties(); // refresh now that builder is finished
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
    if (s.ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.statistics, TABLE_SYNC_MICROS);
      *io_status = file_writer->Sync(ioptions.use_fsync);
    }
    TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
    if (s.ok() && io_status->ok() && !empty) {
      *io_status = file_writer->Close();
    }
    if (s.ok() && io_status->ok() && !empty) {
      // Add the checksum information to file metadata.
      meta->file_checksum = file_writer->GetFileChecksum();
      meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      file_checksum = meta->file_checksum;
      file_checksum_func_name = meta->file_checksum_func_name;
    }

    if (s.ok()) {
      s = *io_status;
    }

    if (blob_file_builder) {
      if (s.ok()) {
        s = blob_file_builder->Finish();
      } else {
        blob_file_builder->Abandon();
      }
      blob_file_builder.reset();
    }

    // TODO Also check the IO status when create the Iterator.

    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // we will regrad this verification as user reads since the goal is
      // to cache it here for further user reads
      ReadOptions read_options;
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          read_options, file_options, internal_comparator, *meta,
          nullptr /* range_del_agg */,
          mutable_cf_options.prefix_extractor.get(), nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          TableReaderCaller::kFlush, /*arena=*/nullptr,
          /*skip_filter=*/false, level,
          MaxFileSizeForL0MetaPin(mutable_cf_options),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key*/ nullptr,
          /*allow_unprepared_value*/ false));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        OutputValidator file_validator(internal_comparator,
                                       /*enable_order_check=*/true,
                                       /*enable_hash=*/true);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          // Generate a rolling 64-bit hash of the key and values
          file_validator.Add(it->key(), it->value()).PermitUncheckedError();
        }
        s = it->status();
        if (s.ok() && !output_validator.CompareValidator(file_validator)) {
          s = Status::Corruption("Paranoid checksums do not match");
        }
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
    ignored.PermitUncheckedError();

    assert(blob_file_additions || blob_file_paths.empty());

    if (blob_file_additions) {
      for (const std::string& blob_file_path : blob_file_paths) {
        ignored = DeleteDBFile(&db_options, blob_file_path, dbname,
                               /*force_bg=*/false, /*force_fg=*/false);
        ignored.PermitUncheckedError();
        TEST_SYNC_POINT("BuildTable::AfterDeleteFile");
      }
    }
  }

  if (meta->fd.GetFileSize() == 0) {
    fname = "(nil)";
  }
  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, column_family_name, fname,
      job_id, meta->fd, meta->oldest_blob_file_number, tp, reason, s,
      file_checksum, file_checksum_func_name);

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
