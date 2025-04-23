#pragma once
#include <vector>
#include <algorithm>
#include <iterator>
#include <numeric>
#include <string>
#include <functional>
#include <utility>
#include <iostream>
#include "DiskManager.hpp"      // see below
#include "DiskBPlusTreeNode.hpp"
#include "SplitResult.hpp"
#include "Interval.h"

template<typename Key, int n, typename Compare = std::less<Key>>
class BPlusTree {
public:
    using Node     = DiskBPlusTreeNode<Key,n>;
    using Result   = std::vector<std::pair<Key,int>>;

    explicit BPlusTree(const std::string &filename = "bptree.dat")
    : _rootOffset(-1)
    , _disk(filename)
    , rowCount(0)
    {}

    // Insert one (key, recordIndex)
    void insert(const Key key, int recordIndex) {
        if (_rootOffset < 0) {
            // first time: create an empty leaf
            Node node;
            node.isLeaf = true;
            node.info[n] = -1;    // next ptr
            _rootOffset = _disk.writeNode(node);
        }
        auto split = insertRecursive(_rootOffset, key, recordIndex);
        if (split) {
            // root split → new root
            Node newRoot;
            newRoot.isLeaf  = false;
            newRoot.numKeys = 1;
            newRoot.setKey(0,split->separator);
            newRoot.info[0] = _rootOffset;
            newRoot.info[1] = split->newNodeOffset;
            _rootOffset = _disk.writeNode(newRoot);
            delete split;
        }
        rowCount++;
    }

    // Range search [start..end]
    Result searchRange(const Key start, const Key end, bool gotEnd = true) {
        Result results;
        if (_rootOffset < 0) return results;

        // 1) descend to leaf
        int offset = _rootOffset;
        Node curr = _disk.readNode(offset);
        while (!curr.isLeaf) {
            // find child index
            std::vector<Key> keysVec;
            keysVec.reserve(curr.numKeys);
            for (int j = 0; j < curr.numKeys; j++) {
                keysVec.push_back(curr.getKey(j));
            }

            // 2) find the first key >= start
            auto pos = std::lower_bound(keysVec.begin(), keysVec.end(), start, Compare{});
            // compute child index
            int i = int(pos - keysVec.begin());
            offset = curr.info[i];
            curr   = _disk.readNode(offset);
        }

        // 2) scan leaf chain
        while (true) {
            for (int i = 0; i < curr.numKeys; i++) {
                auto k = curr.getKey(i);
                if (gotEnd && Compare{}(end, k))      // k > end ?
                    return results;
                if (!Compare{}(k, start))   // k >= start ?
                    results.emplace_back(k, curr.info[i]);
            }
            int next = curr.info[n];
            if (next < 0) break;
            curr = _disk.readNode(next);
        }
        return results;
    }

    /* Finds all records whose key == value.
    // Internally just performs a closed‐interval [value,value] search.
    Result equality_search(const Key value) {
        return searchRange(value, value);
    }*/

    // 1) Closed‐closed: [start, end]
    std::vector<int> rangeClosedClosed(Key start, Key end, bool gotEnd = true) {
        auto all = searchRange(start, end, gotEnd);   // returns std::vector<std::pair<Key,int>>
        std::vector<int> out;
        out.reserve(all.size());
        for (auto &pr : all) {
            out.push_back(pr.second);
        }
        if (gotEnd)
        {
            std::cout << "rangeClosedClosed[" << start << "," << end << "] -> "<< out.size() << " results\n";
        }
        return out;
    }

    // 2) Closed‐open: [start, end)
    std::vector<int> rangeClosedOpen(Key start, Key end) {
        auto all = searchRange(start, end);
        std::vector<int> out;
        out.reserve(all.size());
        for (auto &pr : all) {
            if (pr.first < end)
                out.push_back(pr.second);
        }
        std::cout << "rangeClosedOpen[" << start << "," << end << ") -> " << out.size() << " results\n";
        return out;
    }

    // 3) Open‐closed: (start, end]
    std::vector<int> rangeOpenClosed(Key start, Key end, bool gotEnd = true) {
        auto all = searchRange(start, end, gotEnd);
        std::vector<int> out;
        out.reserve(all.size());
        for (auto &pr : all) {
            if (pr.first > start)
                out.push_back(pr.second);
        }
        if (gotEnd)
        {
            std::cout << "rangeOpenClosed(" << start << "," << end << "] -> " << out.size() << " results\n";
        }
        return out;
    }

    // 4) Open‐open: (start, end)
    std::vector<int> rangeOpenOpen(Key start, Key end) {
        auto all = searchRange(start, end);
        std::vector<int> out;
        out.reserve(all.size());
        for (auto &pr : all) {
            if (pr.first > start && pr.first < end)
                out.push_back(pr.second);
        }
        std::cout << "rangeOpenOpen(" << start << "," << end << ") -> " << out.size() << " results\n";
        return out;
    }

    // [start, ) — closed at start, unbounded end
    std::vector<int> rangeUnboundedStartClosed(Key start) {
        auto out = rangeClosedClosed(start, start, false);
        std::cout << "rangeUnboundedStartClosed[" << start << ",) -> " << out.size() << " results\n";
         return out;
    }

    // (start, ) — open at start, unbounded end
    std::vector<int> rangeUnboundedStartOpen(Key start) {
        auto out = rangeOpenClosed(start, start, false);
        std::cout << "rangeUnboundedStartOpen(" << start << ",) -> " << out.size() << " results\n";
         return out;
    }

    // [ , end]: unbounded start, closed end
    std::vector<int> rangeUnboundedEndClosed(Key end) {
        // build full set of record IDs [0..rowCount-1]
        std::vector<int> full(rowCount);
        std::iota(full.begin(), full.end(), 0);

        // find all IDs with key > end ⇒ (end, ∞)
        auto gt = rangeUnboundedStartOpen(end);
        std::sort(gt.begin(), gt.end());
        gt.erase(std::unique(gt.begin(), gt.end()), gt.end());

        // full \ gt ⇒ all keys ≤ end
        std::vector<int> out;
        std::set_difference(
            full.begin(), full.end(),
            gt.begin(),   gt.end(),
            std::back_inserter(out)
        );
        std::cout << "rangeUnboundedEndClosed(, " << end << "] -> " << out.size() << " results\n";
        return out;
    }
    
    // [ , end): unbounded start, open end
    std::vector<int> rangeUnboundedEndOpen(Key end) {
        // build full set of record IDs [0..rowCount-1]
        std::vector<int> full(rowCount);
        std::iota(full.begin(), full.end(), 0);

        // find all IDs with key ≥ end ⇒ [end, ∞)
        auto ge = rangeUnboundedStartClosed(end);
        std::sort(ge.begin(), ge.end());
        ge.erase(std::unique(ge.begin(), ge.end()), ge.end());

        // full \ ge ⇒ all keys < end
        std::vector<int> out;
        std::set_difference(
            full.begin(), full.end(),
            ge.begin(),   ge.end(),
            std::back_inserter(out)
        );
        std::cout << "rangeUnboundedEndOpen(, " << end << ") -> " << out.size() << " results\n";
        return out;
    }

    // 3) The multi‑interval search
    std::vector<int> searchIntervals(const std::vector<Interval<Key>>& intervals = {}) {
        // 1) No intervals ⇒ return all record IDs [0..rowCount-1]
        if (intervals.empty()) {
            std::vector<int> out(rowCount);
            std::iota(out.begin(), out.end(), 0);
            return out;
        }

        // 2) Otherwise, collect all matching record IDs…
        std::vector<int> ids;
        for (auto const& iv : intervals) {
            std::vector<int> part;
            switch (iv.type) {
                case IntervalType::ClosedClosed:
                    part = rangeClosedClosed(iv.start, iv.end);   break;
                case IntervalType::ClosedOpen:
                    part = rangeClosedOpen(iv.start, iv.end);     break;
                case IntervalType::OpenClosed:
                    part = rangeOpenClosed(iv.start, iv.end);     break;
                case IntervalType::OpenOpen:
                    part = rangeOpenOpen(iv.start, iv.end);       break;
                // new unbounded cases:
                case IntervalType::UpToClosed:
                    part = rangeUnboundedEndClosed(iv.end);       break;
                case IntervalType::UpToOpen:
                    part = rangeUnboundedEndOpen(iv.end);         break;
                case IntervalType::FromClosed:
                    part = rangeUnboundedStartClosed(iv.start);   break;
                case IntervalType::FromOpen:
                    part = rangeUnboundedStartOpen(iv.start);     break;
            }
            ids.insert(ids.end(), part.begin(), part.end());
        }

        // 3) Sort and unique the IDs
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        return ids;
    }


private:
    // recursive insert: returns a heap‐allocated SplitResult if this node splits
    SplitResult<Key>* insertRecursive(int offset, const Key key, int recordIndex)
    {
        Node node = _disk.readNode(offset);

        if (node.isLeaf) {
            // --- leaf insertion ---
            std::vector<Key> keysVec;
            keysVec.reserve(node.numKeys);
            for (int j = 0; j < node.numKeys; j++) {
                keysVec.push_back(node.getKey(j));
            }
            std::vector<int>   recIdx(node.info, node.info + node.numKeys);

            auto pos = std::lower_bound(keysVec.begin(), keysVec.end(), key, Compare{});
            int idx = int(pos - keysVec.begin());
            keysVec.insert(pos, key);
            recIdx.insert(recIdx.begin() + idx, recordIndex);

            if (int(keysVec.size()) <= n) {
                // no split
                node.numKeys = int(keysVec.size());
                for (int i = 0; i < node.numKeys; i++) {
                    node.setKey(i, keysVec[i]);
                    node.info[i] = recIdx[i];
                }
                // next pointer already in info[n]
                _disk.updateNode(offset, node);
                return nullptr;
            }

            // leaf overflow → split into L / R
            int total = int(keysVec.size());       // n+1
            int L     = (total + 1) / 2;           // ceil((n+1)/2)
            int R     = total - L;

            Node right;
            right.isLeaf  = true;
            right.numKeys = R;
            // copy R keys & recIdx
            for (int j = 0; j < R; j++) {
                right.setKey(j, keysVec[L + j]);
                right.info[j] = recIdx[L + j];
            }
            // chain pointers
            right.info[n] = node.info[n];

            int rightOffset = _disk.writeNode(right);

            // shrink left node
            node.numKeys = L;
            for (int j = 0; j < L; j++) {
                node.setKey(j, keysVec[j]);
                node.info[j] = recIdx[j];
            }
            node.info[n] = rightOffset;
            _disk.updateNode(offset, node);

            auto *sr = new SplitResult<Key>{};
            sr->separator     = right.getKey(0);
            sr->newNodeOffset = rightOffset;
            return sr;
        }
        else {
            // --- internal node case ---
            std::vector<Key> keysVec;
            keysVec.reserve(node.numKeys);
            for (int j = 0; j < node.numKeys; j++) {
                keysVec.push_back(node.getKey(j));
            }
            std::vector<int> kids(node.info,  node.info + node.numKeys + 1);

            int i = std::upper_bound(keysVec.begin(), keysVec.end(), key, Compare{}) - keysVec.begin();

            auto *childSplit = insertRecursive(kids[i], key, recordIndex);
            if (!childSplit) return nullptr;

            // insert new separator & child
            keysVec.insert(keysVec.begin() + i, childSplit->separator);
            kids.insert(kids.begin() + i + 1, childSplit->newNodeOffset);

            if (int(keysVec.size()) <= n) {
                // no overflow
                node.numKeys = int(keysVec.size());
                for (int j = 0; j < node.numKeys; j++)
                    node.setKey(j, keysVec[j]);
                for (int j = 0; j < int(kids.size()); j++)
                    node.info[j] = kids[j];
                _disk.updateNode(offset, node);
                delete childSplit;
                return nullptr;
            }

            // internal overflow → split
            int total = int(keysVec.size());   // n+1
            int L     = (n + 1) / 2;           // ceil(n/2) 
            Key sep  = keysVec[L];

            // left side: [0..L-1], kids[0..L]
            // right side: [L+1..end], kids[L+1..end]
            Node right;
            right.isLeaf  = false;
            right.numKeys = total - (L + 1);

            for (int j = 0; j < L; j++)
                node.setKey(j, keysVec[j]);
            for (int j = 0; j < L+1; j++)
                node.info[j] = kids[j];
            node.numKeys = L;
            _disk.updateNode(offset, node);

            for (int j = 0; j < right.numKeys; j++)
                right.setKey(j, keysVec[L + 1 + j]);
            for (int j = 0; j < int(kids.size()) - (L + 1); j++)
                right.info[j] = kids[L + 1 + j];
            int rightOffset = _disk.writeNode(right);

            auto *sr = new SplitResult<Key>{};
            sr->separator     = sep;
            sr->newNodeOffset = rightOffset;
            delete childSplit;
            return sr;
        }
    }

    int _rootOffset;
    DiskManager<Node> _disk;
    size_t rowCount; 
};
