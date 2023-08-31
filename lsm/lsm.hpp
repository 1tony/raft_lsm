
//    lsm.hpp
//    lsm-tree
//
//    Created by Aron Szanto on 3/3/17.


//    sLSM: Skiplist-Based LSM Tree
//    Copyright © 2017 Aron Szanto. All rights reserved.
//
//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//        
//        You should have received a copy of the GNU General Public License
//        along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
#pragma once

#ifndef LSM_H
#define LSM_H

#include "run.hpp"
#include "skipList.hpp"
//#include "bloom.hpp"
#include "diskLevel.hpp"
//#include"hashMap.hpp"
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <stdlib.h>
#include <future>
#include <vector>
#include <mutex>
#include <thread>
#include <stdint.h>
#include <climits>
template <class K, class V>
class LSM {
    
    typedef SkipList<K,V> RunType;
    
    
    
public:
    V V_TOMBSTONE = (V) TOMBSTONE;
    mutex *mergeLock;
    mutex *domerger;//在do_merge时如果出现并发，不进行限制，可能会造成数据不安全，后期可以结合信号量来实现并发操作
    
    vector<Run<K,V> *> C_0;
    
    //vector<BloomFilter<K> *> filters;
    std::unordered_map<K,V> lsm_log;
    vector<DiskLevel<K,V> *> diskLevels;
    
    LSM<K,V>(const LSM<K,V> &other) = default;
    LSM<K,V>(LSM<K,V> &&other) = default;
    
    LSM<K,V>(unsigned long eltsPerRun, unsigned int numRuns, double merged_frac, unsigned int pageSize, unsigned int diskRunsPerLevel): _eltsPerRun(eltsPerRun), _num_runs(numRuns), _frac_runs_merged(merged_frac), _diskRunsPerLevel(diskRunsPerLevel), _num_to_merge(ceil(_frac_runs_merged * _num_runs)), _pageSize(pageSize){
    /*
    @eltsPerRun:（内存中）run的元素个数，存储到该值，就会替换到下一个run
    @numRuns:内存中的跳表数量，不是硬盘区的diskrun
    @merged_frac://合并频率
    @pageSize://page页的可以存储多少元素个数
    @diskRunsPerLevel://当前level下可以装多少个run
    */
        _activeRun = 0;
        _n = 0;
        DiskLevel<K,V> * diskLevel = new DiskLevel<K, V>(pageSize, 1, _num_to_merge * _eltsPerRun, _diskRunsPerLevel, ceil(_diskRunsPerLevel * _frac_runs_merged));
        diskLevels.push_back(diskLevel);
        _numDiskLevels = 1;
        
        for (int i = 0; i < _num_runs; i++){
            RunType * run = new RunType(INT32_MIN,INT32_MAX);//这里是new的一个跳表
            run->set_size(_eltsPerRun);//设置一个跳表的大小
            //C_0中存放了num_run个跳表
            C_0.push_back(run);//然后将该跳表插入到队列中
        }
        mergeLock = new mutex();
    }
    ~LSM<K,V>(){
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        delete mergeLock;
        for (int i = 0; i < C_0.size(); ++i){
            delete C_0[i];
        }
        for (int i = 0; i < diskLevels.size(); ++i){
            delete diskLevels[i];
        }
    }
    
    std::unordered_map<K,V> &get_log(){return lsm_log;}

    void insert_key(K &key, V &value) {
        //@param:input：就是正常的键值对

        //插入前做一些判断
        //C_0看作一张表，类型：vector<Run<K,V> *>
        //当一个跳表装满了，就换一个跳表
        if (C_0[_activeRun]->num_elements() >= _eltsPerRun){
            ++_activeRun;
        }
        
        if (_activeRun >= _num_runs){
            //如果活跃的activeRun够大了，就do_merage
            do_merge();
        }
        C_0[_activeRun]->insert_key(key,value);
        lsm_log[key] = value;//存放到log中
    }
    
    bool lookup(K &key, V &value){
        bool found = false;
        //查询的时候，就对所有跳表进行遍历
        for (int i = _activeRun; i >= 0; --i){
            //查询跳表的范围
            if (key < C_0[i]->get_min() || key > C_0[i]->get_max())
                continue;
            value = C_0[i]->lookup(key, found);
            if (found) {
                return value != V_TOMBSTONE;//判断是不是无效值
            }
        }
        //如果跳表没有，就找disk，在disk之前，先将merge线程结束
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        for (int i = 0; i < _numDiskLevels; i++){
            
            value = diskLevels[i]->lookup(key, found);//lookup是disk_level自己函数
            if (found) {
                return value != V_TOMBSTONE;
            }
        }
        return false;
    }
    
    void delete_key(K &key){
        insert_key(key, V_TOMBSTONE);//删除值，就是插入一个最小默认值
        lsm_log.erase(key);
    }
    
    vector<KVPair<K,V>> range(K &key1, K &key2){
        if (key2 <= key1){
            return (vector<KVPair<K,V>> {});
        }
        
        vector<KVPair<K,V>> eltsInRange = vector<KVPair<K,V>>();

        //先从跳表中开始查找
        for (int i = _activeRun; i >= 0; --i){
            //分别对跳表进行查找
            vector<KVPair<K,V>> cur_elts = C_0[i]->get_all_in_range(key1, key2);
            if (cur_elts.size() != 0){
                //存在，就更改一下容量
                eltsInRange.reserve(eltsInRange.size() + cur_elts.size()); //this over-reserves to be safe
                for (int c = 0; c < cur_elts.size(); c++){
                    if (cur_elts[c].value != V_TOMBSTONE){
                        eltsInRange.push_back(cur_elts[c]);
                    }
                    
                }
            }
            
        }
        //跳表中无法查询，则向disk中查找
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        
        for (int j = 0; j < _numDiskLevels; j++){
            for (int r = diskLevels[j]->_activeRun - 1; r >= 0 ; --r){
                unsigned long i1, i2;
                diskLevels[j]->runs[r]->range(key1, key2, i1, i2);
                if (i2 - i1 != 0){
                    auto oldSize = eltsInRange.size();
                    eltsInRange.reserve(oldSize + (i2 - i1)); // also over-reserves space
                    for (unsigned long m = i1; m < i2; ++m){
                        auto KV = diskLevels[j]->runs[r]->map[m];
                        if (KV.value != V_TOMBSTONE) {    
                            eltsInRange.push_back(KV);
                        }
                    }
                }
            }
        }
        
        return eltsInRange;
    }
    
    
    
    void printElts(){
        if (mergeThread.joinable())
            mergeThread.join();
        cout << "MEMORY BUFFER" << endl;
        for (int i = 0; i <= _activeRun; i++){
            cout << "MEMORY BUFFER RUN " << i << endl;
            auto all = C_0[i]->get_all();
            for (KVPair<K, V> &c : all) {
                cout << c.key << ":" << c.value << " ";
            }
            cout << endl;
            
        }
        
        cout << "\nDISK BUFFER" << endl;
        for (int i = 0; i < _numDiskLevels; i++){
            cout << "DISK LEVEL " << i << endl;
            for (int j = 0; j < diskLevels[i]->_activeRun; j++){
                cout << "RUN " << j << endl;
                for (int k = 0; k < diskLevels[i]->runs[j]->getCapacity(); k++){
                    cout << diskLevels[i]->runs[j]->map[k].key << ":" << diskLevels[i]->runs[j]->map[k].value << " ";
                }
                cout << endl;
            }
            cout << endl;
        }
        
    }
    void printStats(){
        cout << "Number of Elements: " << size() << endl;
        cout << "Number of Elements in Buffer (including deletes): " << num_buffer() << endl;
        
        for (int i = 0; i < diskLevels.size(); ++i){
            cout << "Number of Elements in Disk Level " << i << "(including deletes): " << diskLevels[i]->num_elements() << endl;
        }
        cout << "KEY VALUE DUMP BY LEVEL: " << endl;
        printElts();
    }
    
    //private: // TODO MAKE PRIVATE
    unsigned int _activeRun;
    unsigned long _eltsPerRun;
    double _bfFalsePositiveRate;
    unsigned int _num_runs;
    double _frac_runs_merged;
    unsigned int _numDiskLevels;
    unsigned int _diskRunsPerLevel;
    unsigned int _num_to_merge;
    unsigned int _pageSize;
    unsigned long _n;
    thread mergeThread;
    
    void mergeRunsToLevel(int level) {
        //这里就是对disklevel构成的地方，这里disklevel会mergerToLevel
        bool isLast = false;
        
        if (level == _numDiskLevels){ 
            DiskLevel<K,V> * newLevel = new DiskLevel<K, V>(_pageSize, level + 1, diskLevels[level - 1]->_runSize * diskLevels[level - 1]->_mergeSize, _diskRunsPerLevel, 50);
            diskLevels.push_back(newLevel);
            _numDiskLevels++;
        }
        
        if (diskLevels[level]->levelFull()) {
            mergeRunsToLevel(level + 1); 
        }
        
        if(level + 1 == _numDiskLevels && diskLevels[level]->levelEmpty()){
            isLast = true;
        }
        
        
        vector<DiskRun<K, V> *> runsToMerge = diskLevels[level - 1]->getRunsToMerge();
        unsigned long runLen = diskLevels[level - 1]->_runSize;
        diskLevels[level]->addRuns(runsToMerge, runLen, isLast);//将新合并的数据，装入disk中
        diskLevels[level - 1]->freeMergedRuns(runsToMerge);
    }

    void merge_runs(vector<Run<K,V>*> runs_to_merge){
        vector<KVPair<K, V>> to_merge = vector<KVPair<K,V>>();
        to_merge.reserve(_eltsPerRun * _num_to_merge);//这个容量计算的标准是什么？

        for (int i = 0; i < runs_to_merge.size(); i++){
            auto all = (runs_to_merge)[i]->get_all();
            
            to_merge.insert(to_merge.begin(), all.begin(), all.end());//将所有元素插入到merge中
            delete (runs_to_merge)[i];
        }
        
        sort(to_merge.begin(), to_merge.end());
        mergeLock->lock();
        //放入disklevel中
        if (diskLevels[0]->levelFull()){//如果disk满了，对disk中的数据进行merge
            mergeRunsToLevel(1); //这里表示0层之后就是1层，意思多个level吗？
        }
        diskLevels[0]->addRunByArray(&to_merge[0], to_merge.size());//放进disk中
        mergeLock->unlock();
    }
    
    void do_merge(){
        if (_num_to_merge == 0)
            return;
        vector<Run<K,V>*> runs_to_merge = vector<Run<K,V>*>();//构建一个跳表组
        for (int i = 0; i < _num_to_merge; i++){
            runs_to_merge.push_back(C_0[i]);
            //bf_to_merge.push_back(filters[i]);
        }
        //判断是否可joinable,如果可以就join,等待merge线程执行结束再继续执行
        if (mergeThread.joinable()){
            mergeThread.join();
        }
        //创建一个新的merge线程
        mergeThread = thread (&LSM::merge_runs, this, runs_to_merge);
        //转移后，将其删除，erase删除后，数组中的元素都要向前移位
        C_0.erase(C_0.begin(), C_0.begin() + _num_to_merge);
        
        _activeRun -= _num_to_merge;//合并了_num_to_merge，则活跃表减少
        for (int i = _activeRun; i < _num_runs; i++){
            RunType * run = new RunType(INT32_MIN,INT32_MAX);
            run->set_size(_eltsPerRun);
            C_0.push_back(run);

        }
    }
    unsigned long num_buffer(){
        if (mergeThread.joinable())
            mergeThread.join();
        unsigned long total = 0;
        for (int i = 0; i <= _activeRun; ++i)
            total += C_0[i]->num_elements();
        return total;
    }
    unsigned long size(){
        K min = INT_MIN;
        K max = INT_MAX;
        auto r = range(min, max);
        return r.size();
    }

};




#endif /* lsm_h */

