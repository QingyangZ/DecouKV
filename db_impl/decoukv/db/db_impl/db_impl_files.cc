//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>
#include <set>
#include <unordered_set>

#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "db/memtable_list.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

uint64_t DBImpl::MinLogNumberToKeep() {
  if (allow_2pc()) {
    return versions_->min_log_number_to_keep_2pc();
  } else {
    return versions_->MinLogNumberWithUnflushedData();
  }
}

uint64_t DBImpl::MinObsoleteSstNumberToKeep() {
  mutex_.AssertHeld();
  if (!pending_outputs_.empty()) {
    return *pending_outputs_.begin();
  }
  return std::numeric_limits<uint64_t>::max();
}

Status DBImpl::DisableFileDeletions() {
  InstrumentedMutexLock l(&mutex_);
  return DisableFileDeletionsWithLock();
}

Status DBImpl::DisableFileDeletionsWithLock() {
  mutex_.AssertHeld();
  ++disable_delete_obsolete_files_;
  if (disable_delete_obsolete_files_ == 1) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Disabled");
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "File Deletions Disabled, but already disabled. Counter: %d",
                   disable_delete_obsolete_files_);
  }
  return Status::OK();
}

Status DBImpl::EnableFileDeletions(bool force) {
  // Job id == 0 means that this is not our background process, but rather
  // user thread
  JobContext job_context(0);
  int saved_counter;  // initialize on all paths
  {
    InstrumentedMutexLock l(&mutex_);
    if (force) {
      // if force, we need to enable file deletions right away
      disable_delete_obsolete_files_ = 0;
    } else if (disable_delete_obsolete_files_ > 0) {
      --disable_delete_obsolete_files_;
    }
    saved_counter = disable_delete_obsolete_files_;
    if (saved_counter == 0) {
      FindObsoleteFiles(&job_context, true);
      bg_cv_.SignalAll();
    }
  }
  if (saved_counter == 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Enabled");
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "File Deletions Enable, but not really enabled. Counter: %d",
                   saved_counter);
  }
  job_context.Clean();
  LogFlush(immutable_db_options_.info_log);
  return Status::OK();
}

bool DBImpl::IsFileDeletionsEnabled() const {
  return 0 == disable_delete_obsolete_files_;
}

// * Returns the list of live files in 'sst_live' and 'blob_live'.
// If it's doing full scan:
// * Returns the list of all files in the filesystem in
// 'full_scan_candidate_files'.
// Otherwise, gets obsolete files from VersionSet.
// no_full_scan = true -- never do the full scan using GetChildren()
// force = false -- don't force the full scan, except every
//  mutable_db_options_.delete_obsolete_files_period_micros
// force = true -- force the full scan
void DBImpl::FindObsoleteFiles(JobContext* job_context, bool force,
                               bool no_full_scan) {
  mutex_.AssertHeld();

  // if deletion is disabled, do nothing
  if (disable_delete_obsolete_files_ > 0) {
    return;
  }

  bool doing_the_full_scan = false;

  // logic for figuring out if we're doing the full scan
  if (no_full_scan) {
    doing_the_full_scan = false;
  } else if (force ||
             mutable_db_options_.delete_obsolete_files_period_micros == 0) {
    doing_the_full_scan = true;
  } else {
    const uint64_t now_micros = immutable_db_options_.clock->NowMicros();
    if ((delete_obsolete_files_last_run_ +
         mutable_db_options_.delete_obsolete_files_period_micros) <
        now_micros) {
      doing_the_full_scan = true;
      delete_obsolete_files_last_run_ = now_micros;
    }
  }

  // don't delete files that might be currently written to from compaction
  // threads
  // Since job_context->min_pending_output is set, until file scan finishes,
  // mutex_ cannot be released. Otherwise, we might see no min_pending_output
  // here but later find newer generated unfinalized files while scanning.
  job_context->min_pending_output = MinObsoleteSstNumberToKeep();

  // Get obsolete files.  This function will also update the list of
  // pending files in VersionSet().
  versions_->GetObsoleteFiles(
      &job_context->sst_delete_files, &job_context->blob_delete_files,
      &job_context->manifest_delete_files, job_context->min_pending_output);

  // Mark the elements in job_context->sst_delete_files and
  // job_context->blob_delete_files as "grabbed for purge" so that other threads
  // calling FindObsoleteFiles with full_scan=true will not add these files to
  // candidate list for purge.
  for (const auto& sst_to_del : job_context->sst_delete_files) {
    MarkAsGrabbedForPurge(sst_to_del.metadata->fd.GetNumber());
  }

  for (const auto& blob_file : job_context->blob_delete_files) {
    MarkAsGrabbedForPurge(blob_file.GetBlobFileNumber());
  }

  // store the current filenum, lognum, etc
  job_context->manifest_file_number = versions_->manifest_file_number();
  job_context->pending_manifest_file_number =
      versions_->pending_manifest_file_number();
  job_context->log_number = MinLogNumberToKeep();
  job_context->prev_log_number = versions_->prev_log_number();

  versions_->AddLiveFiles(&job_context->sst_live, &job_context->blob_live);
  if (doing_the_full_scan) {
    InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                  dbname_);
    std::set<std::string> paths;
    for (size_t path_id = 0; path_id < immutable_db_options_.db_paths.size();
         path_id++) {
      paths.insert(immutable_db_options_.db_paths[path_id].path);
    }

    // Note that if cf_paths is not specified in the ColumnFamilyOptions
    // of a particular column family, we use db_paths as the cf_paths
    // setting. Hence, there can be multiple duplicates of files from db_paths
    // in the following code. The duplicate are removed while identifying
    // unique files in PurgeObsoleteFiles.
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      for (size_t path_id = 0; path_id < cfd->ioptions()->cf_paths.size();
           path_id++) {
        auto& path = cfd->ioptions()->cf_paths[path_id].path;

        if (paths.find(path) == paths.end()) {
          paths.insert(path);
        }
      }
    }

    for (auto& path : paths) {
      // set of all files in the directory. We'll exclude files that are still
      // alive in the subsequent processings.
      std::vector<std::string> files;
      Status s = env_->GetChildren(path, &files);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (const std::string& file : files) {
        uint64_t number;
        FileType type;
        // 1. If we cannot parse the file name, we skip;
        // 2. If the file with file_number equals number has already been
        // grabbed for purge by another compaction job, or it has already been
        // schedule for purge, we also skip it if we
        // are doing full scan in order to avoid double deletion of the same
        // file under race conditions. See
        // https://github.com/facebook/rocksdb/issues/3573
        if (!ParseFileName(file, &number, info_log_prefix.prefix, &type) ||
            !ShouldPurge(number)) {
          continue;
        }

        // TODO(icanadi) clean up this mess to avoid having one-off "/"
        // prefixes
        job_context->full_scan_candidate_files.emplace_back("/" + file, path);
      }
    }

    // Add log files in wal_dir
    if (immutable_db_options_.wal_dir != dbname_) {
      std::vector<std::string> log_files;
      Status s = env_->GetChildren(immutable_db_options_.wal_dir, &log_files);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (const std::string& log_file : log_files) {
        job_context->full_scan_candidate_files.emplace_back(
            log_file, immutable_db_options_.wal_dir);
      }
    }
    // Add info log files in db_log_dir
    if (!immutable_db_options_.db_log_dir.empty() &&
        immutable_db_options_.db_log_dir != dbname_) {
      std::vector<std::string> info_log_files;
      Status s =
          env_->GetChildren(immutable_db_options_.db_log_dir, &info_log_files);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (std::string& log_file : info_log_files) {
        job_context->full_scan_candidate_files.emplace_back(
            log_file, immutable_db_options_.db_log_dir);
      }
    }
  }

  // logs_ is empty when called during recovery, in which case there can't yet
  // be any tracked obsolete logs
  if (!alive_log_files_.empty() && !logs_.empty()) {
    uint64_t min_log_number = job_context->log_number;
    size_t num_alive_log_files = alive_log_files_.size();
    // find newly obsoleted log files
    while (alive_log_files_.begin()->number < min_log_number) {
      auto& earliest = *alive_log_files_.begin();
      if (immutable_db_options_.recycle_log_file_num >
          log_recycle_files_.size()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "adding log %" PRIu64 " to recycle list\n",
                       earliest.number);
        log_recycle_files_.push_back(earliest.number);
      } else {
        job_context->log_delete_files.push_back(earliest.number);
      }
      if (job_context->size_log_to_delete == 0) {
        job_context->prev_total_log_size = total_log_size_;
        job_context->num_alive_log_files = num_alive_log_files;
      }
      job_context->size_log_to_delete += earliest.size;
      total_log_size_ -= earliest.size;
      if (two_write_queues_) {
        log_write_mutex_.Lock();
      }
      alive_log_files_.pop_front();
      if (two_write_queues_) {
        log_write_mutex_.Unlock();
      }
      // Current log should always stay alive since it can't have
      // number < MinLogNumber().
      assert(alive_log_files_.size());
    }
    while (!logs_.empty() && logs_.front().number < min_log_number) {
      auto& log = logs_.front();
      if (log.getting_synced) {
        log_sync_cv_.Wait();
        // logs_ could have changed while we were waiting.
        continue;
      }
      logs_to_free_.push_back(log.ReleaseWriter());
      {
        InstrumentedMutexLock wl(&log_write_mutex_);
        logs_.pop_front();
      }
    }
    // Current log cannot be obsolete.
    assert(!logs_.empty());
  }

  // We're just cleaning up for DB::Write().
  assert(job_context->logs_to_free.empty());
  job_context->logs_to_free = logs_to_free_;
  job_context->log_recycle_files.assign(log_recycle_files_.begin(),
                                        log_recycle_files_.end());
  if (job_context->HaveSomethingToDelete()) {
    ++pending_purge_obsolete_files_;
  }
  logs_to_free_.clear();
}

namespace {
bool CompareCandidateFile(const JobContext::CandidateFileInfo& first,
                          const JobContext::CandidateFileInfo& second) {
  if (first.file_name > second.file_name) {
    return true;
  } else if (first.file_name < second.file_name) {
    return false;
  } else {
    return (first.file_path > second.file_path);
  }
}
};  // namespace

// Delete obsolete files and log status and information of file deletion
void DBImpl::DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                                    const std::string& path_to_sync,
                                    FileType type, uint64_t number) {
  TEST_SYNC_POINT_CALLBACK("DBImpl::DeleteObsoleteFileImpl::BeforeDeletion",
                           const_cast<std::string*>(&fname));

  Status file_deletion_status;
  if (type == kTableFile || type == kBlobFile || type == kWalFile) {
    file_deletion_status =
        DeleteDBFile(&immutable_db_options_, fname, path_to_sync,
                     /*force_bg=*/false, /*force_fg=*/!wal_in_db_path_);
  } else {
    file_deletion_status = env_->DeleteFile(fname);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::DeleteObsoleteFileImpl:AfterDeletion",
                           &file_deletion_status);
  if (file_deletion_status.ok()) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[JOB %d] Delete %s type=%d #%" PRIu64 " -- %s\n", job_id,
                    fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  } else if (env_->FileExists(fname).IsNotFound()) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "[JOB %d] Tried to delete a non-existing file %s type=%d #%" PRIu64
        " -- %s\n",
        job_id, fname.c_str(), type, number,
        file_deletion_status.ToString().c_str());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "[JOB %d] Failed to delete %s type=%d #%" PRIu64 " -- %s\n",
                    job_id, fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  }
  if (type == kTableFile) {
    EventHelpers::LogAndNotifyTableFileDeletion(
        &event_logger_, job_id, number, fname, file_deletion_status, GetName(),
        immutable_db_options_.listeners);
  }
}

// Diffs the files listed in filenames and those that do not
// belong to live files are possibly removed. Also, removes all the
// files in sst_delete_files and log_delete_files.
// It is not necessary to hold the mutex when invoking this method.
void DBImpl::PurgeObsoleteFiles(JobContext& state, bool schedule_only) {
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:Begin");
  // we'd better have sth to delete
  assert(state.HaveSomethingToDelete());

  // FindObsoleteFiles() should've populated this so nonzero
  assert(state.manifest_file_number != 0);

  // Now, convert lists to unordered sets, WITHOUT mutex held; set is slow.
  std::unordered_set<uint64_t> sst_live_set(state.sst_live.begin(),
                                            state.sst_live.end());
  std::unordered_set<uint64_t> blob_live_set(state.blob_live.begin(),
                                             state.blob_live.end());
  std::unordered_set<uint64_t> log_recycle_files_set(
      state.log_recycle_files.begin(), state.log_recycle_files.end());

  auto candidate_files = state.full_scan_candidate_files;
  candidate_files.reserve(
      candidate_files.size() + state.sst_delete_files.size() +
      state.blob_delete_files.size() + state.log_delete_files.size() +
      state.manifest_delete_files.size());
  // We may ignore the dbname when generating the file names.
  for (auto& file : state.sst_delete_files) {
    candidate_files.emplace_back(
        MakeTableFileName(file.metadata->fd.GetNumber()), file.path);
    if (file.metadata->table_reader_handle) {
      table_cache_->Release(file.metadata->table_reader_handle);
    }
    file.DeleteMetadata();
  }

  for (const auto& blob_file : state.blob_delete_files) {
    candidate_files.emplace_back(BlobFileName(blob_file.GetBlobFileNumber()),
                                 blob_file.GetPath());
  }

  for (auto file_num : state.log_delete_files) {
    if (file_num > 0) {
      candidate_files.emplace_back(LogFileName(file_num),
                                   immutable_db_options_.wal_dir);
    }
  }
  for (const auto& filename : state.manifest_delete_files) {
    candidate_files.emplace_back(filename, dbname_);
  }

  // dedup state.candidate_files so we don't try to delete the same
  // file twice
  std::sort(candidate_files.begin(), candidate_files.end(),
            CompareCandidateFile);
  candidate_files.erase(
      std::unique(candidate_files.begin(), candidate_files.end()),
      candidate_files.end());

  if (state.prev_total_log_size > 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[JOB %d] Try to delete WAL files size %" PRIu64
                   ", prev total WAL file size %" PRIu64
                   ", number of live WAL files %" ROCKSDB_PRIszt ".\n",
                   state.job_id, state.size_log_to_delete,
                   state.prev_total_log_size, state.num_alive_log_files);
  }

  std::vector<std::string> old_info_log_files;
  InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                dbname_);

  // File numbers of most recent two OPTIONS file in candidate_files (found in
  // previos FindObsoleteFiles(full_scan=true))
  // At this point, there must not be any duplicate file numbers in
  // candidate_files.
  uint64_t optsfile_num1 = std::numeric_limits<uint64_t>::min();
  uint64_t optsfile_num2 = std::numeric_limits<uint64_t>::min();
  for (const auto& candidate_file : candidate_files) {
    const std::string& fname = candidate_file.file_name;
    uint64_t number;
    FileType type;
    if (!ParseFileName(fname, &number, info_log_prefix.prefix, &type) ||
        type != kOptionsFile) {
      continue;
    }
    if (number > optsfile_num1) {
      optsfile_num2 = optsfile_num1;
      optsfile_num1 = number;
    } else if (number > optsfile_num2) {
      optsfile_num2 = number;
    }
  }

  // Close WALs before trying to delete them.
  for (const auto w : state.logs_to_free) {
    // TODO: maybe check the return value of Close.
    auto s = w->Close();
    s.PermitUncheckedError();
  }

  bool own_files = OwnTablesAndLogs();
  std::unordered_set<uint64_t> files_to_del;
  for (const auto& candidate_file : candidate_files) {
    const std::string& to_delete = candidate_file.file_name;
    uint64_t number;
    FileType type;
    // Ignore file if we cannot recognize it.
    if (!ParseFileName(to_delete, &number, info_log_prefix.prefix, &type)) {
      continue;
    }

    bool keep = true;
    switch (type) {
      case kWalFile:
        keep = ((number >= state.log_number) ||
                (number == state.prev_log_number) ||
                (log_recycle_files_set.find(number) !=
                 log_recycle_files_set.end()));
        break;
      case kDescriptorFile:
        // Keep my manifest file, and any newer incarnations'
        // (can happen during manifest roll)
        keep = (number >= state.manifest_file_number);
        break;
      case kTableFile:
        // If the second condition is not there, this makes
        // DontDeletePendingOutputs fail
        keep = (sst_live_set.find(number) != sst_live_set.end()) ||
               number >= state.min_pending_output;
        if (!keep) {
          files_to_del.insert(number);
        }
        break;
      case kBlobFile:
        keep = number >= state.min_pending_output ||
               (blob_live_set.find(number) != blob_live_set.end());
        if (!keep) {
          files_to_del.insert(number);
        }
        break;
      case kTempFile:
        // Any temp files that are currently being written to must
        // be recorded in pending_outputs_, which is inserted into "live".
        // Also, SetCurrentFile creates a temp file when writing out new
        // manifest, which is equal to state.pending_manifest_file_number. We
        // should not delete that file
        //
        // TODO(yhchiang): carefully modify the third condition to safely
        //                 remove the temp options files.
        keep = (sst_live_set.find(number) != sst_live_set.end()) ||
               (blob_live_set.find(number) != blob_live_set.end()) ||
               (number == state.pending_manifest_file_number) ||
               (to_delete.find(kOptionsFileNamePrefix) != std::string::npos);
        break;
      case kInfoLogFile:
        keep = true;
        if (number != 0) {
          old_info_log_files.push_back(to_delete);
        }
        break;
      case kOptionsFile:
        keep = (number >= optsfile_num2);
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:1",
            reinterpret_cast<void*>(&number));
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:2",
            reinterpret_cast<void*>(&keep));
        break;
      case kCurrentFile:
      case kDBLockFile:
      case kIdentityFile:
      case kMetaDatabase:
        keep = true;
        break;
    }

    if (keep) {
      continue;
    }

    std::string fname;
    std::string dir_to_sync;
    if (type == kTableFile) {
      // evict from cache
      TableCache::Evict(table_cache_.get(), number);
      fname = MakeTableFileName(candidate_file.file_path, number);
      dir_to_sync = candidate_file.file_path;
    } else if (type == kBlobFile) {
      fname = BlobFileName(candidate_file.file_path, number);
      dir_to_sync = candidate_file.file_path;
    } else {
      dir_to_sync =
          (type == kWalFile) ? immutable_db_options_.wal_dir : dbname_;
      fname = dir_to_sync +
              ((!dir_to_sync.empty() && dir_to_sync.back() == '/') ||
                       (!to_delete.empty() && to_delete.front() == '/')
                   ? ""
                   : "/") +
              to_delete;
    }

#ifndef ROCKSDB_LITE
    if (type == kWalFile && (immutable_db_options_.wal_ttl_seconds > 0 ||
                             immutable_db_options_.wal_size_limit_mb > 0)) {
      wal_manager_.ArchiveWALFile(fname, number);
      continue;
    }
#endif  // !ROCKSDB_LITE

    // If I do not own these files, e.g. secondary instance with max_open_files
    // = -1, then no need to delete or schedule delete these files since they
    // will be removed by their owner, e.g. the primary instance.
    if (!own_files) {
      continue;
    }
    if (schedule_only) {
      InstrumentedMutexLock guard_lock(&mutex_);
      SchedulePendingPurge(fname, dir_to_sync, type, number, state.job_id);
    } else {
      DeleteObsoleteFileImpl(state.job_id, fname, dir_to_sync, type, number);
    }
  }

  {
    // After purging obsolete files, remove them from files_grabbed_for_purge_.
    InstrumentedMutexLock guard_lock(&mutex_);
    autovector<uint64_t> to_be_removed;
    for (auto fn : files_grabbed_for_purge_) {
      if (files_to_del.count(fn) != 0) {
        to_be_removed.emplace_back(fn);
      }
    }
    for (auto fn : to_be_removed) {
      files_grabbed_for_purge_.erase(fn);
    }
  }

  // Delete old info log files.
  size_t old_info_log_file_count = old_info_log_files.size();
  if (old_info_log_file_count != 0 &&
      old_info_log_file_count >= immutable_db_options_.keep_log_file_num) {
    std::sort(old_info_log_files.begin(), old_info_log_files.end());
    size_t end =
        old_info_log_file_count - immutable_db_options_.keep_log_file_num;
    for (unsigned int i = 0; i <= end; i++) {
      std::string& to_delete = old_info_log_files.at(i);
      std::string full_path_to_delete =
          (immutable_db_options_.db_log_dir.empty()
               ? dbname_
               : immutable_db_options_.db_log_dir) +
          "/" + to_delete;
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Delete info log file %s\n", state.job_id,
                     full_path_to_delete.c_str());
      Status s = env_->DeleteFile(full_path_to_delete);
      if (!s.ok()) {
        if (env_->FileExists(full_path_to_delete).IsNotFound()) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[JOB %d] Tried to delete non-existing info log file %s FAILED "
              "-- %s\n",
              state.job_id, to_delete.c_str(), s.ToString().c_str());
        } else {
          ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                          "[JOB %d] Delete info log file %s FAILED -- %s\n",
                          state.job_id, to_delete.c_str(),
                          s.ToString().c_str());
        }
      }
    }
  }

  for (const auto& blob_file : state.blob_delete_files) {
    auto it2 = blob_files_.find(blob_file.GetBlobFileNumber());
    if (it2 == blob_files_.end()) {
      return;
    }
    auto blob = it2->second;
    total_blob_size_ -= blob->GetFileSize();
    // std::cout << total_blob_size_ << std::endl;
  }

#ifndef ROCKSDB_LITE
  wal_manager_.PurgeObsoleteWALFiles();
#endif  // ROCKSDB_LITE
  LogFlush(immutable_db_options_.info_log);
  InstrumentedMutexLock l(&mutex_);
  --pending_purge_obsolete_files_;
  assert(pending_purge_obsolete_files_ >= 0);
  if (pending_purge_obsolete_files_ == 0) {
    bg_cv_.SignalAll();
  }
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:End");
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();
  JobContext job_context(next_job_id_.fetch_add(1));
  FindObsoleteFiles(&job_context, true);

  mutex_.Unlock();
  if (job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  mutex_.Lock();
}

uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const ColumnFamilyData* cfd_to_flush,
    const autovector<MemTable*>& memtables_to_flush) {
  uint64_t min_log = 0;

  // we must look through the memtables for two phase transactions
  // that have been committed but not yet flushed
  std::unordered_set<MemTable*> memtables_to_flush_set(
      memtables_to_flush.begin(), memtables_to_flush.end());
  for (auto loop_cfd : *vset->GetColumnFamilySet()) {
    if (loop_cfd->IsDropped() || loop_cfd == cfd_to_flush) {
      continue;
    }

    auto log = loop_cfd->imm()->PrecomputeMinLogContainingPrepSection(
        &memtables_to_flush_set);

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }

    log = loop_cfd->mem()->GetMinLogContainingPrepSection();

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<const autovector<MemTable*>*>& memtables_to_flush) {
  uint64_t min_log = 0;

  std::unordered_set<ColumnFamilyData*> cfds_to_flush_set(cfds_to_flush.begin(),
                                                          cfds_to_flush.end());
  std::unordered_set<MemTable*> memtables_to_flush_set;
  for (const autovector<MemTable*>* memtables : memtables_to_flush) {
    memtables_to_flush_set.insert(memtables->begin(), memtables->end());
  }
  for (auto loop_cfd : *vset->GetColumnFamilySet()) {
    if (loop_cfd->IsDropped() || cfds_to_flush_set.count(loop_cfd)) {
      continue;
    }

    auto log = loop_cfd->imm()->PrecomputeMinLogContainingPrepSection(
        &memtables_to_flush_set);
    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }

    log = loop_cfd->mem()->GetMinLogContainingPrepSection();
    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list) {
  assert(vset != nullptr);

  // Precompute the min log number containing unflushed data for the column
  // family being flushed (`cfd_to_flush`).
  uint64_t cf_min_log_number_to_keep = 0;
  for (auto& e : edit_list) {
    if (e->HasLogNumber()) {
      cf_min_log_number_to_keep =
          std::max(cf_min_log_number_to_keep, e->GetLogNumber());
    }
  }
  if (cf_min_log_number_to_keep == 0) {
    // No version edit contains information on log number. The log number
    // for this column family should stay the same as it is.
    cf_min_log_number_to_keep = cfd_to_flush.GetLogNumber();
  }

  // Get min log number containing unflushed data for other column families.
  uint64_t min_log_number_to_keep =
      vset->PreComputeMinLogNumberWithUnflushedData(&cfd_to_flush);
  if (cf_min_log_number_to_keep != 0) {
    min_log_number_to_keep =
        std::min(cf_min_log_number_to_keep, min_log_number_to_keep);
  }
  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists) {
  assert(vset != nullptr);
  assert(!cfds_to_flush.empty());
  assert(cfds_to_flush.size() == edit_lists.size());

  uint64_t min_log_number_to_keep = port::kMaxUint64;
  for (const auto& edit_list : edit_lists) {
    uint64_t log = 0;
    for (const auto& e : edit_list) {
      if (e->HasLogNumber()) {
        log = std::max(log, e->GetLogNumber());
      }
    }
    if (log != 0) {
      min_log_number_to_keep = std::min(min_log_number_to_keep, log);
    }
  }
  if (min_log_number_to_keep == port::kMaxUint64) {
    min_log_number_to_keep = cfds_to_flush[0]->GetLogNumber();
    for (size_t i = 1; i < cfds_to_flush.size(); i++) {
      min_log_number_to_keep =
          std::min(min_log_number_to_keep, cfds_to_flush[i]->GetLogNumber());
    }
  }

  std::unordered_set<const ColumnFamilyData*> flushed_cfds(
      cfds_to_flush.begin(), cfds_to_flush.end());
  min_log_number_to_keep =
      std::min(min_log_number_to_keep,
               vset->PreComputeMinLogNumberWithUnflushedData(flushed_cfds));

  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list,
    const autovector<MemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker) {
  assert(vset != nullptr);
  assert(prep_tracker != nullptr);
  // Calculate updated min_log_number_to_keep
  // Since the function should only be called in 2pc mode, log number in
  // the version edit should be sufficient.

  uint64_t min_log_number_to_keep =
      PrecomputeMinLogNumberToKeepNon2PC(vset, cfd_to_flush, edit_list);

  // if are 2pc we must consider logs containing prepared
  // sections of outstanding transactions.
  //
  // We must check min logs with outstanding prep before we check
  // logs references by memtables because a log referenced by the
  // first data structure could transition to the second under us.
  //
  // TODO: iterating over all column families under db mutex.
  // should find more optimal solution
  auto min_log_in_prep_heap =
      prep_tracker->FindMinLogContainingOutstandingPrep();

  if (min_log_in_prep_heap != 0 &&
      min_log_in_prep_heap < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_in_prep_heap;
  }

  uint64_t min_log_refed_by_mem = FindMinPrepLogReferencedByMemTable(
      vset, &cfd_to_flush, memtables_to_flush);

  if (min_log_refed_by_mem != 0 &&
      min_log_refed_by_mem < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_refed_by_mem;
  }
  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    const autovector<const autovector<MemTable*>*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker) {
  assert(vset != nullptr);
  assert(prep_tracker != nullptr);
  assert(cfds_to_flush.size() == edit_lists.size());
  assert(cfds_to_flush.size() == memtables_to_flush.size());

  uint64_t min_log_number_to_keep =
      PrecomputeMinLogNumberToKeepNon2PC(vset, cfds_to_flush, edit_lists);

  uint64_t min_log_in_prep_heap =
      prep_tracker->FindMinLogContainingOutstandingPrep();

  if (min_log_in_prep_heap != 0 &&
      min_log_in_prep_heap < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_in_prep_heap;
  }

  uint64_t min_log_refed_by_mem = FindMinPrepLogReferencedByMemTable(
      vset, cfds_to_flush, memtables_to_flush);

  if (min_log_refed_by_mem != 0 &&
      min_log_refed_by_mem < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_refed_by_mem;
  }

  return min_log_number_to_keep;
}

Status DBImpl::SetDBId(bool read_only) {
  Status s;
  // Happens when immutable_db_options_.write_dbid_to_manifest is set to true
  // the very first time.
  if (db_id_.empty()) {
    // Check for the IDENTITY file and create it if not there.
    s = fs_->FileExists(IdentityFileName(dbname_), IOOptions(), nullptr);
    // Typically Identity file is created in NewDB() and for some reason if
    // it is no longer available then at this point DB ID is not in Identity
    // file or Manifest.
    if (s.IsNotFound()) {
      // Create a new DB ID, saving to file only if allowed
      if (read_only) {
        db_id_ = env_->GenerateUniqueId();
        return Status::OK();
      } else {
        s = SetIdentityFile(env_, dbname_);
        if (!s.ok()) {
          return s;
        }
      }
    } else if (!s.ok()) {
      assert(s.IsIOError());
      return s;
    }
    s = GetDbIdentityFromIdentityFile(&db_id_);
    if (immutable_db_options_.write_dbid_to_manifest && s.ok()) {
      VersionEdit edit;
      edit.SetDBId(db_id_);
      Options options;
      MutableCFOptions mutable_cf_options(options);
      versions_->db_id_ = db_id_;
      s = versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                                 mutable_cf_options, &edit, &mutex_, nullptr,
                                 /* new_descriptor_log */ false);
    }
  } else if (!read_only) {
    s = SetIdentityFile(env_, dbname_, db_id_);
  }
  return s;
}

Status DBImpl::DeleteUnreferencedSstFiles() {
  mutex_.AssertHeld();
  std::vector<std::string> paths;
  paths.push_back(NormalizePath(dbname_ + std::string(1, kFilePathSeparator)));
  for (const auto& db_path : immutable_db_options_.db_paths) {
    paths.push_back(
        NormalizePath(db_path.path + std::string(1, kFilePathSeparator)));
  }
  for (const auto* cfd : *versions_->GetColumnFamilySet()) {
    for (const auto& cf_path : cfd->ioptions()->cf_paths) {
      paths.push_back(
          NormalizePath(cf_path.path + std::string(1, kFilePathSeparator)));
    }
  }
  // Dedup paths
  std::sort(paths.begin(), paths.end());
  paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

  uint64_t next_file_number = versions_->current_next_file_number();
  uint64_t largest_file_number = next_file_number;
  std::set<std::string> files_to_delete;
  Status s;
  for (const auto& path : paths) {
    std::vector<std::string> files;
    s = env_->GetChildren(path, &files);
    if (!s.ok()) {
      break;
    }
    for (const auto& fname : files) {
      uint64_t number = 0;
      FileType type;
      if (!ParseFileName(fname, &number, &type)) {
        continue;
      }
      // path ends with '/' or '\\'
      const std::string normalized_fpath = path + fname;
      largest_file_number = std::max(largest_file_number, number);
      if (type == kTableFile && number >= next_file_number &&
          files_to_delete.find(normalized_fpath) == files_to_delete.end()) {
        files_to_delete.insert(normalized_fpath);
      }
    }
  }
  if (!s.ok()) {
    return s;
  }

  if (largest_file_number > next_file_number) {
    versions_->next_file_number_.store(largest_file_number + 1);
  }

  VersionEdit edit;
  edit.SetNextFile(versions_->next_file_number_.load());
  assert(versions_->GetColumnFamilySet());
  ColumnFamilyData* default_cfd = versions_->GetColumnFamilySet()->GetDefault();
  assert(default_cfd);
  s = versions_->LogAndApply(
      default_cfd, *default_cfd->GetLatestMutableCFOptions(), &edit, &mutex_,
      directories_.GetDbDir(), /*new_descriptor_log*/ false);
  if (!s.ok()) {
    return s;
  }

  mutex_.Unlock();
  for (const auto& fname : files_to_delete) {
    s = env_->DeleteFile(fname);
    if (!s.ok()) {
      break;
    }
  }
  mutex_.Lock();
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
