//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// 1. 找到这个页在缓冲池中的位置
// 2. 写入磁盘 
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end() || page_id == INVALID_PAGE_ID){
    latch_.unlock();
    return false;
  }
  
  frame_id_t flush_fid = iter->second;
  disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(int i = 0; i < pool_size_;i++){
    auto frame = &pages_[i];
    if(frame->page_id_ == INVALID_PAGE_ID){
      continue;
    }

    disk_manager_->WritePage(frame->GetPageId(),frame->GetData());
    frame->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  return nullptr;
}

// 获取一个page
// 1. 如果该页在缓冲池中 
// 2. 如果该页不在缓冲池中
//  2.1 找freelist
//  2.2 找replacer
// 有就返回page 并更新page信息
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(page_id);
  if(iter != page_table_.end()){
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];

    page->pin_count_++;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 如果该page不存在 要么在freelist 找 要么 replacer 找
  frame_id_t frame_id = -1;
  Page *p = nullptr;
  if (!free_list_.empty()){
    // list在结尾操作效率高一些
    frame_id = free_list_.back();
    free_list_.pop_back();
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
    // 数组方式寻址
    p = pages_ + frame_id;
  }else{
    bool victimized = replacer_->Victim(&frame_id);
    if (!victimized){
      latch_.unlock();
      return nullptr;
    }
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
    p = pages_ + frame_id;
    
    // 2.     If R is dirty, write it back to the disk.
    if(p->IsDirty()){
      disk_manager_->WritePage(p->GetPageId(),p->GetData());
      p->is_dirty_ = false;
    }
    p->pin_count_ = 0;
  }
  
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(p->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  p->page_id_ = page_id;
  p->ResetMemory();
  disk_manager_->ReadPage(page_id,p->GetData());
  p->pin_count_++;
  latch_.unlock();
  return p;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  latch_.lock();
  if(page_table_.count(page_id)==0){
    latch_.unlock();
    return true;
  }
  
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  auto frame_id = page_table_[page_id];
  auto frame = &pages_[frame_id];
  if(frame->GetPinCount() > 0){
    latch_.unlock();
    return false;
  }

  
  disk_manager_->DeallocatePage(page_id);

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { return false; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
