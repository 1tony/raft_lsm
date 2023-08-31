//
//  diskRun.hpp
//  lsm-tree
//
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
#ifndef diskRun_h
#define diskRun_h
#include <vector>
#include <cstdint>
#include <cstring>
#include <string>
#include "run.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>
#include<iostream>

using namespace std;

template <class K, class V> class DiskLevel;

template <class K, class V>
class DiskRun {
    friend class DiskLevel<K,V>;
public:
    typedef KVPair<K,V> KVPair_t;

    //存放到disk中，需要维护一个新的diskrun
    static int compareKVs (const void * a, const void * b)
    {
        if ( *(KVPair<K,V>*)a <  *(KVPair<K,V>*)b ) return -1;
        if ( *(KVPair<K,V>*)a == *(KVPair<K,V>*)b ) return 0;
        if ( *(KVPair<K,V>*)a >  *(KVPair<K,V>*)b ) return 1;
        return 10;
    }
    
    
    KVPair_t *map;
    int fd;
    unsigned int pageSize;
    //BloomFilter<K> bf;
    
    K minKey = INT8_MIN;
    K maxKey = INT8_MIN;
    
    //每一个run都是一个小容器，我们对其容量相关进行存储
    DiskRun<K,V> (unsigned long capacity, unsigned int pageSize, int level, int runID):_capacity(capacity),_level(level), _iMaxFP(0), pageSize(pageSize), _runID(runID) {
        
        _filename = "C_" + to_string(level) + "_" + to_string(runID) + ".txt"; //根据level和runid来构建文件名称
        
        size_t filesize = capacity * sizeof(KVPair_t);//获取需要存储的容量大小
        
        long result;
        
        fd = open(_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600); //创建文件
        if (fd == -1) {
            perror("Error opening file for writing");
            exit(EXIT_FAILURE);
        }
        
        /* Stretch the file size to the size of the (mmapped) array of KVPairs
         */
        result = lseek(fd, filesize - 1, SEEK_SET); //获取文件指针，获取容量的上一个字符的偏移指针
        if (result == -1) {
            close(fd);
            perror("Error calling lseek() to 'stretch' the file");
            exit(EXIT_FAILURE);
        }
        
        
        result = write(fd, "", 1); //向文件中写一个空字节
        if (result != 1) {
            close(fd);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
        
        
        map = (KVPair<K, V>*) mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);//获取文件的内存区域指针
        if (map == MAP_FAILED) {
            close(fd);
            perror("Error mmapping the file");
            exit(EXIT_FAILURE);
        }
    }
    ~DiskRun<K,V>(){
        fsync(fd);
        doUnmap();
        
        if (remove(_filename.c_str())){
            perror(("Error removing file " + string(_filename)).c_str());
            exit(EXIT_FAILURE);
        }
    }
    void setCapacity(unsigned long newCap){
        _capacity = newCap;
    }
    unsigned long getCapacity(){
        return _capacity;
    }
    void writeData(const KVPair_t *run, const size_t offset, const unsigned long len) {
        //直接将其转移到另一个块内存中
        memcpy(map + offset, run, len * sizeof(KVPair_t));
        _capacity = len;
        
    }
    void constructIndex(){
        _fencePointers.reserve(_capacity / pageSize);//容量/pagesize = page的页数
        _iMaxFP = -1; 
        for (int j = 0; j < _capacity; j++) { //将所有页都遍历一遍：bf好像是用来记录日志的
            //bf.add((K*) &map[j].key, sizeof(K)); //获取获取已经被转移的key值
            if (j % pageSize == 0){
                _fencePointers.push_back(map[j].key); 
                _iMaxFP++;//指针+1
            }
        }
        if (_iMaxFP >= 0){
            _fencePointers.resize(_iMaxFP + 1); //计算完成后，就重置数组的大小
        }
        
        //记录上最大和最小的key值，
        minKey = map[0].key;
        maxKey = map[_capacity - 1].key;
    }
    
    unsigned long binary_search (const unsigned long offset, const unsigned long n, const K &key, bool &found) {
        if (n == 0){
            found = true;
            return offset;
        }
        unsigned long min = offset, max = offset + n - 1;
        unsigned long middle = (min + max) >> 1;
        while (min <= max) {
            if (key > map[middle].key)
                min = middle + 1;
            else if (key == map[middle].key) {
                found = true;
                return middle;
            }
            else
                max = middle - 1;
            middle = (min + max) >> 1;
            
        }
        return min;
    }
    
    void get_flanking_FP(const K &key, unsigned long &start, unsigned long &end){
        //找到key值对应page页的起始和结束位置
        if (_iMaxFP == 0) {
            start = 0;
            end = _capacity;
        }
        else if (key < _fencePointers[1]){
            start = 0;
            end = pageSize;
        }
        else if (key >= _fencePointers[_iMaxFP]) {
            start = _iMaxFP * pageSize;
            end = _capacity;
        }
        else {
            unsigned min = 0, max = _iMaxFP;
            while (min < max) {
                
                unsigned middle = (min + max) >> 1;
                if (key > _fencePointers[middle]){
                    if (key < _fencePointers[middle + 1]){
                        start = middle * pageSize;
                        end = (middle + 1) * pageSize;
                        return; 
                    }
                    min = middle + 1;
                }
                else if (key < _fencePointers[middle]) {
                    if (key >= _fencePointers[middle - 1]){
                        start = (middle - 1) * pageSize;
                        end = middle * pageSize;
                        return; 
                    }
                    max = middle - 1;
                }
                
                else {
                    start = middle * pageSize;
                    end = start;
                    return;
                }
                
            }

        }
    }
    
    unsigned long get_index(const K &key, bool &found){
        unsigned  long start, end;
        get_flanking_FP(key, start, end); //找到key所在页的start和end
        //先在找到对应页，再在页内找
        unsigned long ret = binary_search(start, end - start, key, found); //找到具体key值得索引
        return ret;
    }
    
     V lookup(const K &key, bool &found){
        //查询函数：找map中的索引，再返回对应的value值，如果不存在就nullptr
         unsigned long idx = get_index(key, found);
         V ret = map[idx].value;
         return found ? ret : (V) NULL;
     }
    
    void range(const K &key1, const K &key2, unsigned long &i1, unsigned long &i2){
        i1 = 0;
        i2 = 0;
        //判断这个数在不在这个范围
        if (key1 > maxKey || key2 < minKey){
            return;
        }
        if (key1 >= minKey){
            bool found = false;
            i1 = get_index(key1, found);
            
        }
        if (key2 > maxKey){
            i2 = _capacity;
            return;
        }
        else {
            bool found = false;
            i2 = get_index(key2, found);
        }
    }
    
    void printElts(){
        for (int j = 0; j < _capacity; j++){
            std::cout << map[j].key << " ";
        }
        std::cout << std::endl;
    }
    
private:
    unsigned long _capacity; //容量大小
    string _filename;//对应的文件名称
    int _level;
    vector<K> _fencePointers;
    unsigned _iMaxFP;
    unsigned _runID;//每一个diskrun都有一个ID
    //double _bf_fp;
                            
    void doMap(){
        
        size_t filesize = _capacity * sizeof(KVPair_t);
        // 构建map的映射
        fd = open(_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
        if (fd == -1) {
            perror("Error opening file for writing");
            exit(EXIT_FAILURE);
        }
        
        //所以说disklevel的数据还是在内存中
        map = (KVPair<K, V>*) mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (map == MAP_FAILED) {
            close(fd);
            perror("Error mmapping the file");
            exit(EXIT_FAILURE);
        }
    }
    
    void doUnmap(){
        size_t filesize = _capacity * sizeof(KVPair_t);

        //解除映射关系
        if (munmap(map, filesize) == -1) {
            perror("Error un-mmapping the file");
        }
        
        close(fd);
        fd = -5;
    }
    
    void doubleSize(){
        unsigned long new_capacity = _capacity * 2;
        //扩展内存大小，这里需要做一个重新的映射关系
        size_t new_filesize = new_capacity * sizeof(KVPair_t);
        int result = lseek(fd, new_filesize - 1, SEEK_SET);
        if (result == -1) {
            close(fd);
            perror("Error calling lseek() to 'stretch' the file");
            exit(EXIT_FAILURE);
        }
        
        result = write(fd, "", 1);
        if (result != 1) {
            close(fd);
            perror("Error writing last byte of the file");
            exit(EXIT_FAILURE);
        }
        
        map = (KVPair<K, V>*) mmap(0, new_filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (map == MAP_FAILED) {
            close(fd);
            perror("Error mmapping the file");
            exit(EXIT_FAILURE);
        }
        
        _capacity = new_capacity;
    }
    
    
    
    
};
#endif /* diskRun_h */

