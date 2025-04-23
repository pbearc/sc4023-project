// DiskManager.hpp
#pragma once

#include <fstream>
#include <string>
#include <cstring>
#include "Constants.h"   // defines BLOCK_SIZE

template<typename Node>
class DiskManager {
public:
    // Opens (or creates) the file in binary read/write mode.
    DiskManager(const std::string &filename) {
        file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            // file doesn't exist yet → create it
            file_.clear();
            file_.open(filename, std::ios::out | std::ios::binary);
            file_.close();
            // re‑open for read/write
            file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        }
    }

    ~DiskManager() {
        file_.close();
    }

    // Append `node` as one BLOCK_SIZE chunk; return byte-offset at which it was written
    int writeNode(const Node &node) {
        static_assert(sizeof(Node) <= BLOCK_SIZE,
                      "Node must fit within one BLOCK_SIZE");
        char buffer[BLOCK_SIZE] = {0};
        std::memcpy(buffer, &node, sizeof(Node));

        file_.seekp(0, std::ios::end);
        int offset = static_cast<int>(file_.tellp());
        file_.write(buffer, BLOCK_SIZE);
        file_.flush();
        return offset;
    }

    // Read a BLOCK_SIZE chunk from `offset` into a fresh Node
    Node readNode(int offset) {
        char buffer[BLOCK_SIZE];
        file_.seekg(offset, std::ios::beg);
        file_.read(buffer, BLOCK_SIZE);

        Node node;
        std::memcpy(&node, buffer, sizeof(Node));
        return node;
    }

    // Overwrite the BLOCK_SIZE chunk at `offset` with `node`
    void updateNode(int offset, const Node &node) {
        char buffer[BLOCK_SIZE] = {0};
        std::memcpy(buffer, &node, sizeof(Node));

        file_.seekp(offset, std::ios::beg);
        file_.write(buffer, BLOCK_SIZE);
        file_.flush();
    }

private:
    std::fstream file_;
};
