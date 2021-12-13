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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    // 自动获取和释放锁
    // std:: <std::mutex> lock(latch);
    latch.lock();
    if(lru_map.empty()){
        latch.unlock();
        return false;
    }

    // 选择最少使用得 将其删除
    frame_id_t to_delete = lru_list.back();
    lru_map.erase(to_delete);

    // 从链表中删除
    lru_list.pop_back();
    *frame_id = to_delete;
    latch.unlock();
    return true;
}

// 表示这个frame被某个进程所使用 不能成为牺牲对象 所以从lru中移除
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch);
    if(lru_map.count(frame_id)!=0){
        lru_list.erase(lru_map[frame_id]);
        lru_map.erase(frame_id);
    }
}
// 相当于put函数
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch);

    if(lru_map.count(frame_id)!=0){
        return;
    }

    // ruguo 链表尾部是否有空余节点 如果没有则移除尾部节点 直到有空位
    while(Size()>=capacity){
        frame_id_t to_delete = lru_list.front();
        lru_list.pop_back();
        lru_map.erase(to_delete);
    }

    // 插入
    lru_list.push_front(frame_id);

    // 更新
    lru_map[frame_id] = lru_list.begin();
}

size_t LRUReplacer::Size() { return lru_list.size(); }

}  // namespace bustub
