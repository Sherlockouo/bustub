//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages):capacity_(num_pages){
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    // 自动获取和释放锁
    // std:: <std::mutex> lock(latch_);
    latch_.lock();
    if(lru_map_.empty()){
        latch_.unlock();
        return false;
    }

    // 选择最少使用得 将其删除
    frame_id_t to_delete = lru_list_.back();
    lru_map_.erase(to_delete);

    // 从链表中删除
    lru_list_.pop_back();
    *frame_id = to_delete;
    latch_.unlock();
    return true;
}

// 表示这个frame被某个进程所使用 不能成为牺牲对象 所以从lru中移除
void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    if(lru_map_.count(frame_id)!=0){
        lru_list_.erase(lru_map_[frame_id]);
        lru_map_.erase(frame_id);
    }
    latch_.unlock();
}
// 相当于put函数
void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();    
    // LOG_DEBUG("lru point 1");
    if(lru_map_.count(frame_id)!=0){
        latch_.unlock();    
        return;
    }

    // LOG_DEBUG("lru point 2");
    // ruguo 链表尾部是否有空余节点 如果没有则移除尾部节点 直到有空位
    while(Size()>=capacity_){

        // LOG_INFO("size is %zu, capacity_ is %zu", Size(), capacity_);
        
        frame_id_t to_delete = lru_list_.back();
        
        // LOG_INFO("frame_id_t %d size is %zu ",to_delete, lru_list_.size());
        lru_list_.pop_back();
        // frame_id_t to_delete = lru_list_.front();
        // lru_list_.pop_front();
        // LOG_INFO("map size is %zu ", lru_map_.size());
        lru_map_.erase(to_delete);
    }

    // 插入
    lru_list_.push_front(frame_id);

    // 更新
    lru_map_[frame_id] = lru_list_.begin();
    latch_.unlock();
}

size_t LRUReplacer::Size() { 
    latch_.lock();
    return lru_map_.size(); 
    latch_.unlock();
}

}  // namespace bustub
