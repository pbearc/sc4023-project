#ifndef COLUMN_STORE_H
#define COLUMN_STORE_H

#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <filesystem>
#include <cstring>
#include <cmath>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <utility>
#include <cctype>
#include "Constants.h"
#include <algorithm>
#include <cctype>

inline std::string toUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){return std::toupper(c);});
    return s;
}

class ColumnStore;

// Column base class for polymorphism
class ColumnBase {
public:
    virtual ~ColumnBase() = default;
    virtual void storeToDisk() = 0;
    virtual void loadFromDisk() = 0;
    virtual size_t size() const = 0;
    virtual const std::string& getFileName() const = 0;
    virtual void clear() = 0;
};

// Template class for different types of columns
template <typename T>
class Column : public ColumnBase {
private:
    std::vector<T> data;
    std::string name;
    std::string fullFilePath;

public:
    Column(const std::string& colName, const std::string& path);
    void addValue(const T& value);
    size_t size() const override;
    void clear() override { data.clear(); }
    void storeToDisk() override;
    void loadFromDisk() override; 
    std::vector<std::pair<int, T>> fetchRecords(const std::vector<int>& recordIndices) const;

    const std::vector<T>& getData() const { return data; }

    const std::string& getFileName() const override { return fullFilePath; }
};


// ColumnStore class to manage all columns
class ColumnStore {
private:
    std::string dataFolderPath;
    std::unique_ptr<Column<std::string>> months;
    std::unique_ptr<Column<std::string>> towns;
    std::unique_ptr<Column<std::string>> flatTypes;
    std::unique_ptr<Column<std::string>> blocks;
    std::unique_ptr<Column<std::string>> streetNames;
    std::unique_ptr<Column<std::string>> storeyRanges;
    std::unique_ptr<Column<double>> floorAreas;
    std::unique_ptr<Column<std::string>> flatModels;
    std::unique_ptr<Column<int>> leaseCommenceDates;
    std::unique_ptr<Column<double>> resalePrices;

    size_t rowCount; 

    std::string buildFullPath(const std::string& filename) const;

public:
    explicit ColumnStore(const std::string& folderPath = "data_store");
    void loadFromCSV(const std::string& csvFilename);
    void saveToDisk();
    void loadFromDisk();
    size_t getRowCount() const;
    std::string getDataFolderPath() const { return dataFolderPath; } 

    struct DataRow {
        std::string month;
        std::string town;
        std::string flatType;
        std::string block;
        std::string streetName;
        std::string storeyRange;
        double      floorArea;
        std::string flatModel;
        int         leaseDate;
        double      resalePrice;
    };

    // fetchRows: given a list of record IDs, return (id, DataRow) for each
    std::vector<std::pair<int, DataRow>> fetchRows(const std::vector<int>& recordIndices) const;

    // Public Accessor methods for columns
    const Column<std::string>* getMonths() const { return months.get(); }
    const Column<std::string>* getTowns() const { return towns.get(); }
    const Column<std::string>* getFlatTypes() const { return flatTypes.get(); }
    const Column<std::string>* getBlocks() const { return blocks.get(); }
    const Column<std::string>* getStreetNames() const { return streetNames.get(); }
    const Column<std::string>* getStoreyRanges() const { return storeyRanges.get(); }
    const Column<double>* getFloorAreas() const { return floorAreas.get(); }
    const Column<std::string>* getFlatModels() const { return flatModels.get(); }
    const Column<int>* getLeaseCommenceDates() const { return leaseCommenceDates.get(); }
    const Column<double>* getResalePrices() const { return resalePrices.get(); }
};


template <typename T>
Column<T>::Column(const std::string& colName, const std::string& path)
    : name(colName), fullFilePath(path) {}

template <typename T>
void Column<T>::addValue(const T& value) {
    data.push_back(value);
}

template <typename T>
size_t Column<T>::size() const {
    return data.size();
}


template <> void Column<int>::storeToDisk();
template <> void Column<int>::loadFromDisk();
template <> void Column<double>::storeToDisk();
template <> void Column<double>::loadFromDisk();
template <> void Column<std::string>::storeToDisk();
template <> void Column<std::string>::loadFromDisk();

// Generic implementation for trivially-copyable types (int, double, etc.)
template<typename T>
inline std::vector<std::pair<int, T>> Column<T>::fetchRecords(const std::vector<int>& recordIndices) const {
    std::vector<std::pair<int, T>> out;
    std::ifstream file(fullFilePath, std::ios::binary);
    if (!file) return out;

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (!file) return out;

    const size_t valueSize = sizeof(T);
    const size_t valuesPerBlock = BLOCK_SIZE / valueSize;
    std::unordered_map<size_t, std::vector<int>> blockToIndices;

    // Group indices by block
    for (int idx : recordIndices) {
        if (idx < 0 || static_cast<size_t>(idx) >= count) continue;
        size_t block = idx / valuesPerBlock;
        int  local = idx % valuesPerBlock;
        blockToIndices[block].push_back(local);
    }

    std::vector<char> buffer(BLOCK_SIZE);
    for (auto& kv : blockToIndices) {
        size_t blockNum = kv.first;
        auto const& offs = kv.second;

        // Seek to this block
        file.seekg(sizeof(size_t) + std::streamoff(blockNum) * BLOCK_SIZE, std::ios::beg);
        file.read(buffer.data(), BLOCK_SIZE);
        size_t bytesRead = file.gcount();

        // Extract each requested value
        for (int local : offs) {
            size_t byteOff = local * valueSize;
            if (byteOff + valueSize <= bytesRead) {
                T val;
                std::memcpy(&val, buffer.data() + byteOff, valueSize);
                int recordIdx = static_cast<int>(blockNum * valuesPerBlock + local);
                out.emplace_back(recordIdx, val);
            }
        }
    }
    return out;
}

// Specialization for std::string columns
template<>
inline std::vector<std::pair<int, std::string>> Column<std::string>::fetchRecords(const std::vector<int>& recordIndices) const {
    std::vector<std::pair<int, std::string>> out;
    std::ifstream file(fullFilePath, std::ios::binary);
    if (!file) return out;

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (!file) return out;

    const size_t strPerBlock = BLOCK_SIZE / FIXED_STRING_LEN;
    std::unordered_map<size_t, std::vector<int>> blockToIndices;

    for (int idx : recordIndices) {
        if (idx < 0 || static_cast<size_t>(idx) >= count) continue;
        size_t block = idx / strPerBlock;
        int local = idx % strPerBlock;
        blockToIndices[block].push_back(local);
    }

    std::vector<char> buffer(BLOCK_SIZE);
    for (auto& kv : blockToIndices) {
        size_t blockNum = kv.first;
        auto const& offs = kv.second;

        file.seekg(sizeof(size_t) + std::streamoff(blockNum) * BLOCK_SIZE, std::ios::beg);
        file.read(buffer.data(), BLOCK_SIZE);
        size_t bytesRead = file.gcount();

        for (int local : offs) {
            size_t byteOff = local * FIXED_STRING_LEN;
            if (byteOff < bytesRead) {
                char* ptr = buffer.data() + byteOff;
                size_t len = strnlen(ptr, FIXED_STRING_LEN);
                std::string val(ptr, len);
                int recordIdx = static_cast<int>(blockNum * strPerBlock + local);
                out.emplace_back(recordIdx, val);
            }
        }
    }
    return out;
}


#endif