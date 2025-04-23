#pragma once
#include <cstring>
#include <algorithm>
#include <string>
#include "Constants.h"

// n = max number of keys per node (from Constants.h)
// Key must be trivially copyable (so we can memcpy it in/out of blocks)
template<typename Key, int n>
struct DiskBPlusTreeNode {
    bool     isLeaf;              // leaf vs internal
    int      numKeys;             // how many keys are valid
    Key      keys[n];             // the keys in this node
    int      info[n+1];           // children offsets or record indices (and next‐leaf ptr)

    DiskBPlusTreeNode() : isLeaf(true), numKeys(0) {
        std::memset(keys, 0, sizeof(keys));
        std::fill_n(info, n+1, -1);
    }

    // ─── Uniform accessors for ANY Key ───
    Key getKey(int i) const {
        return keys[i];
    }
    void setKey(int i, const Key k) {
        keys[i] = k;
    }
};

template<int n>
struct DiskBPlusTreeNode<std::string,n> {
    bool   isLeaf;
    int    numKeys;
    char   keys[n][FIXED_STRING_LEN];
    int    info[n+1];

    DiskBPlusTreeNode()
      : isLeaf(true), numKeys(0)
    {
        std::memset(keys, 0, sizeof(keys));
        std::fill_n(info, n+1, -1);
    }

    // ─── String‑aware accessors ───
    std::string getKey(int i) const {
        return std::string(keys[i]);
    }
    void setKey(int i, const std::string &s) {
        // how many chars we actually copy
        size_t copyLen = std::min(s.size(), size_t(FIXED_STRING_LEN - 1));
        // raw copy
        std::memcpy(keys[i], s.data(), copyLen);
        // null‑terminate immediately after the copied characters
        keys[i][copyLen] = '\0';
        // (optional) zero out the rest if you want to avoid junk
        std::memset(keys[i] + copyLen + 1, 0, FIXED_STRING_LEN - copyLen - 1);
    }
};