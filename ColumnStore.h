#ifndef COLUMN_STORE_H
#define COLUMN_STORE_H

#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <filesystem> 


class ColumnStore;

// Column base class for polymorphism
class ColumnBase {
public:
    virtual ~ColumnBase() = default;
    virtual void storeToDisk() = 0;
    virtual void loadFromDisk() = 0;
    virtual size_t size() const = 0;
    virtual const std::string& getFileName() const = 0; // Helper to get filename
};

// Template class for different types of columns
template <typename T>
class Column : public ColumnBase {
private:
    std::vector<T> data;
    std::string name;
    std::string fullFilePath; 

public:
    // Constructor now takes the full path
    Column(const std::string& colName, const std::string& path);
    void addValue(const T& value);
    T getValue(size_t rowIndex) const;
    size_t size() const override;
    void storeToDisk() override;
    void loadFromDisk() override;
    const std::vector<T>& getData() const;
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

    // Helper to build full path
    std::string buildFullPath(const std::string& filename) const;

public:
    // Constructor now takes the folder path
    explicit ColumnStore(const std::string& folderPath = "data_store"); 
    void loadFromCSV(const std::string& csvFilename);
    void saveToDisk();
    void loadFromDisk();
    size_t getRowCount() const;
    std::string getDataFolderPath() const { return dataFolderPath; } // Accessor for folder path

    // Accessor methods for columns
    Column<std::string>* getMonths() { return months.get(); }
    Column<std::string>* getTowns() { return towns.get(); }
    Column<std::string>* getFlatTypes() { return flatTypes.get(); }
    Column<std::string>* getBlocks() { return blocks.get(); }
    Column<std::string>* getStreetNames() { return streetNames.get(); }
    Column<std::string>* getStoreyRanges() { return storeyRanges.get(); }
    Column<double>* getFloorAreas() { return floorAreas.get(); }
    Column<std::string>* getFlatModels() { return flatModels.get(); }
    Column<int>* getLeaseCommenceDates() { return leaseCommenceDates.get(); }
    Column<double>* getResalePrices() { return resalePrices.get(); }
};


template <typename T>
Column<T>::Column(const std::string& colName, const std::string& path)
    : name(colName), fullFilePath(path) {} 

template <typename T>
void Column<T>::addValue(const T& value) {
    data.push_back(value);
}

template <typename T>
T Column<T>::getValue(size_t rowIndex) const {
    if (rowIndex >= data.size()) {
        throw std::out_of_range("Row index out of range for column " + name + " at index " + std::to_string(rowIndex));
    }
    return data[rowIndex];
}

template <typename T>
size_t Column<T>::size() const {
    return data.size();
}

template <typename T>
const std::vector<T>& Column<T>::getData() const {
    return data;
}

#endif