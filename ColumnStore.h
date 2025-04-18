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

const size_t BLOCK_SIZE = 512;
const size_t FIXED_STRING_LEN = 64;

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


#endif