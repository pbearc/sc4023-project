#pragma once

template<typename Key>
struct SplitResult {
    Key separator;        // key to push up
    int newNodeOffset;    // block offset of the new right node
};
