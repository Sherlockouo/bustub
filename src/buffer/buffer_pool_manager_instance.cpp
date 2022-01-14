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
#include "common/logger.h"

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
  // latch_.lock();
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end() || page_id == INVALID_PAGE_ID){
    latch_.unlock();
    return false;
  }
  
  frame_id_t flush_fid = iter->second;
  disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
  // latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(int i = 0; i < static_cast<int>(pool_size_);i++){
    auto frame = &pages_[i];
    if(frame->page_id_ == INVALID_PAGE_ID){
      continue;
    }

    disk_manager_->WritePage(frame->GetPageId(),frame->GetData());
    frame->is_dirty_ = false;
  }
}

// 分配一个新的page 简历page -> frame 映射
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  latch_.lock();
  page_id_t new_page_id = AllocatePage();
  
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool is_all = true;
  for(int i = 0; i < static_cast<int>(pool_size_); i++){
    if(pages_[i].pin_count_==0){
      is_all = false;
      break;
    }
  }
  if(is_all){
    latch_.unlock();
    return nullptr;
  }
  
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t victim_page_id;
  Page *p;
  if(!free_list_.empty()){
    victim_page_id =  free_list_.back();
    free_list_.pop_back();
    // assert(victim_page_id >= 0 && victim_page_id < static_cast<int>(pool_size_));
    // p = pages_+victim_page_id;
  }else{
    victim_page_id = replacer_->Victim(&victim_page_id);
    if(!victim_page_id){
      latch_.unlock();
      return nullptr;
    }
    // assert(victim_page_id >= 0 && victim_page_id < static_cast<int>(pool_size_));
    // p = pages_+victim_page_id;
  }
  
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  assert(victim_page_id >= 0 && victim_page_id < static_cast<int>(pool_size_));
  p = pages_+victim_page_id;
  p->pin_count_ = 0;
  p->page_id_ = new_page_id;
  p->pin_count_++;
  replacer_->Pin(victim_page_id);
  page_table_[new_page_id]= victim_page_id;
  p->is_dirty_ = false;
  *page_id = new_page_id;
  
  // 4.   Set the page ID output parameter. Return a pointer to P.
  disk_manager_->WritePage(p->GetPageId(),p->GetData());

  latch_.unlock();
  return p;
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
  auto frame_id = FindPage(page_id);
  if(frame_id != -1){
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    pages_[frame_id].is_dirty_ = true;
    latch_.unlock();
    return &pages_[frame_id];
  }

  // for ( auto x : page_table_){
  //   std::cout<< x.first <<" "<< x.second<<std::endl;
  // }
  // std::puts("");

  // LOG_INFO("point 1");
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 如果该page不存在 要么在freelist 找 要么 replacer 找

  frame_id = FindFreePage();

  if(frame_id == -1){
    latch_.unlock();
    return nullptr;
  }

  page_table_[page_id] = frame_id;
  replacer_->Pin(page_table_[page_id]);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id,pages_[frame_id].GetData());
  
  latch_.unlock();
  return &pages_[frame_id];
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
  auto page = &pages_[frame_id];
  if(page->GetPinCount() > 0){
    latch_.unlock();
    return false;
  }

  
  DeallocatePage(page_id);

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  
  free_list_.push_back(frame_id);
  
  latch_.unlock();
  return false;
}

// 进程完成了对该页得操作
// 该页又可以被使用
// 将该页放回lru中
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto iter = page_table_.find(page_id);
  // 如果page中没有 返回false
  if (iter == page_table_.end()){
    latch_.unlock();
    return false;
  }
  // LOG_DEBUG("debug point 1");

  // 找到要被unpin的frame
  frame_id_t unpinned_fid = iter->second;
  Page *unpined_page = &pages_[unpinned_fid];

  if(is_dirty){
    unpined_page->is_dirty_ = true;
  }

  // LOG_DEBUG("debug point 2");

  // 如果page pincount为0 直接返回false
  if(unpined_page->pin_count_ == 0){
    latch_.unlock();
    return false;
  }

  // LOG_DEBUG("debug point 3");
  unpined_page->pin_count_--;
  // 如果引用他的人为0 则可以加入lru淘汰队列中
  if(unpined_page->GetPinCount() == 0){
    replacer_->Unpin(unpinned_fid);
  }

  // LOG_DEBUG("debug point 4");
  latch_.unlock();
  return true; 
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

void BufferPoolManagerInstance::FlushPage(page_id_t page_id) {
  auto frame_id = FindPage(page_id);
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
}

frame_id_t BufferPoolManagerInstance::FindFreePage(){
  frame_id_t frame_id;
  if(!free_list_.empty()){
    frame_id = free_list_.back();
    free_list_.pop_back();
    return frame_id;
  }

  if(replacer_->Victim(&frame_id)){
    auto page_id = pages_[frame_id].page_id_;
    FlushPage(page_id);
    page_table_.erase(page_id);
    pages_[frame_id].is_dirty_ = false;
    return frame_id;
  }
  // 无空page
  return -1;  
}

frame_id_t BufferPoolManagerInstance::FindPage(frame_id_t page_id){
  auto iter = page_table_.find(page_id);
  if(iter != page_table_.end()){
    return iter->second;
  }
  return -1;
}

}  // namespace bustub
