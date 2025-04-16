#include "ColumnStore.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>

namespace fs = std::filesystem; 

template <typename T>
void Column<T>::storeToDisk() {
     std::cerr << "Error: storeToDisk not implemented for this type." << std::endl;
}

template <typename T>
void Column<T>::loadFromDisk() {
     std::cerr << "Error: loadFromDisk not implemented for this type." << std::endl;
}

// Template specialization for storing numeric types (int)
template <>
void Column<int>::storeToDisk() {
    std::ofstream file(fullFilePath, std::ios::binary | std::ios::trunc);
    if (!file) {
         std::cerr << "Error: Could not open file for writing: " << fullFilePath << std::endl;
         return; 
    }

    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
    if (count > 0) {
        file.write(reinterpret_cast<const char*>(data.data()), count * sizeof(int));
    }
    // RAII handles closing the file when 'file' goes out of scope
    if (!file) {
        std::cerr << "Error: Failed to write data to file: " << fullFilePath << std::endl;
    }
}

// Template specialization for storing numeric types (double)
template <>
void Column<double>::storeToDisk() {
    std::ofstream file(fullFilePath, std::ios::binary | std::ios::trunc); 
     if (!file) {
         std::cerr << "Error: Could not open file for writing: " << fullFilePath << std::endl;
         return; 
    }

    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
     if (count > 0) {
        file.write(reinterpret_cast<const char*>(data.data()), count * sizeof(double));
     }
     if (!file) {
        std::cerr << "Error: Failed to write data to file: " << fullFilePath << std::endl;
    }
}

// Template specialization for loading numeric types (int)
template <>
void Column<int>::loadFromDisk() {
    data.clear(); 
    std::ifstream file(fullFilePath, std::ios::binary); 

    if (!file) {
        return; 
    }

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (file.gcount() != sizeof(size_t)) { // Check if read was successful
        std::cerr << "Error: Failed to read count from file: " << fullFilePath << std::endl;
        return;
    }


    if (count > 0) {
        data.resize(count); // Resize vector before reading into it
        file.read(reinterpret_cast<char*>(data.data()), count * sizeof(int));
        if (static_cast<size_t>(file.gcount()) != count * sizeof(int)) {
            std::cerr << "Error: Failed to read data fully from file (expected "
                      << count * sizeof(int) << " bytes): " << fullFilePath << std::endl;
            data.clear(); 
        }
    }
}

// Template specialization for loading numeric types (double)
template <>
void Column<double>::loadFromDisk() {
    data.clear();
    std::ifstream file(fullFilePath, std::ios::binary);

    if (!file) {
        return;
    }

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (file.gcount() != sizeof(size_t)) {
        std::cerr << "Error: Failed to read count from file: " << fullFilePath << std::endl;
        return;
    }

    if (count > 0) {
        data.resize(count);
        file.read(reinterpret_cast<char*>(data.data()), count * sizeof(double));
        if (static_cast<size_t>(file.gcount()) != count * sizeof(double)) {
             std::cerr << "Error: Failed to read data fully from file (expected "
                      << count * sizeof(double) << " bytes): " << fullFilePath << std::endl;
            data.clear();
        }
    }
}

// Template specialization for string columns (store)
template <>
void Column<std::string>::storeToDisk() {
    std::ofstream file(fullFilePath, std::ios::binary | std::ios::trunc); 
    if (!file) {
         std::cerr << "Error: Could not open file for writing: " << fullFilePath << std::endl;
         return;
    }

    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(size_t));

    for (const auto& str : data) {
        size_t strLen = str.size();
        file.write(reinterpret_cast<const char*>(&strLen), sizeof(size_t));
        if (strLen > 0) {
            file.write(str.c_str(), strLen);
        }
    }
    if (!file) {
        std::cerr << "Error: Failed to write string data to file: " << fullFilePath << std::endl;
    }
}

// Template specialization for string columns (load)
template <>
void Column<std::string>::loadFromDisk() {
    data.clear();
    std::ifstream file(fullFilePath, std::ios::binary);

    if (!file) {
        return;
    }

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
     if (file.gcount() != sizeof(size_t)) {
        std::cerr << "Error: Failed to read string count from file: " << fullFilePath << std::endl;
        return;
    }

    data.resize(count); // Resize once at the beginning
    for (size_t i = 0; i < count; ++i) {
        size_t strLen = 0;
        file.read(reinterpret_cast<char*>(&strLen), sizeof(size_t));
        if (file.gcount() != sizeof(size_t)) {
             std::cerr << "Error: Failed to read string length at index " << i << " from file: " << fullFilePath << std::endl;
             data.clear(); 
             return;
        }


        if (strLen > 0) {
            data[i].resize(strLen);
            file.read(&data[i][0], strLen); // Read directly into the string's buffer
            if (static_cast<size_t>(file.gcount()) != strLen) {
                std::cerr << "Error: Failed to read string data at index " << i << " (expected "
                          << strLen << " bytes) from file: " << fullFilePath << std::endl;
                data.clear();
                return;
            }
        } else {
             data[i].clear(); 
        }
    }
}

// Helper function to build full path using <filesystem>
std::string ColumnStore::buildFullPath(const std::string& filename) const {
    fs::path dirPath(dataFolderPath);
    fs::path filePath = dirPath / filename; 
    return filePath.string();
}


// Constructor implementation
ColumnStore::ColumnStore(const std::string& folderPath)
    : dataFolderPath(folderPath), rowCount(0)
{
    // Initialize all columns, passing the full path
    months = std::make_unique<Column<std::string>>("months", buildFullPath("col_months.dat"));
    towns = std::make_unique<Column<std::string>>("towns", buildFullPath("col_towns.dat"));
    flatTypes = std::make_unique<Column<std::string>>("flatTypes", buildFullPath("col_flatTypes.dat"));
    blocks = std::make_unique<Column<std::string>>("blocks", buildFullPath("col_blocks.dat"));
    streetNames = std::make_unique<Column<std::string>>("streetNames", buildFullPath("col_streetNames.dat"));
    storeyRanges = std::make_unique<Column<std::string>>("storeyRanges", buildFullPath("col_storeyRanges.dat"));
    floorAreas = std::make_unique<Column<double>>("floorAreas", buildFullPath("col_floorAreas.dat"));
    flatModels = std::make_unique<Column<std::string>>("flatModels", buildFullPath("col_flatModels.dat"));
    leaseCommenceDates = std::make_unique<Column<int>>("leaseCommenceDates", buildFullPath("col_leaseCommenceDates.dat"));
    resalePrices = std::make_unique<Column<double>>("resalePrices", buildFullPath("col_resalePrices.dat"));

}

// Load data from CSV file
void ColumnStore::loadFromCSV(const std::string& csvFilename) {
    std::ifstream file(csvFilename);
    std::string line;

    if (!file) {
        std::cerr << "Error: Could not open CSV file: " << csvFilename << std::endl;
        return; 
    }

    rowCount = 0;

    // Skip header line
    if (!std::getline(file, line)) {
         std::cerr << "Error: Could not read header line from CSV file: " << csvFilename << std::endl;
         return;
    }

    while (std::getline(file, line)) {
        if (line.empty() || line.find_first_not_of(" \t\r\n") == std::string::npos) {
            continue;
        }

        std::stringstream ss(line);
        std::string token;
        int column = 0;
        bool row_ok = true;

        std::vector<std::string> row_tokens(10); // 10 columns

        while (std::getline(ss, token, ',')) {
            if (column < 10) {
                token.erase(0, token.find_first_not_of(" \t\r\n"));
                token.erase(token.find_last_not_of(" \t\r\n") + 1);
                row_tokens[column] = token;
            } else {
                std::cerr << "Warning: Extra columns detected in CSV row: " << line << std::endl;
            }
            column++;
        }

        if (column != 10) {
            std::cerr << "Warning: Incorrect number of columns (" << column << "/10) in CSV row: " << line << std::endl;
            row_ok = false;
        }

        if (row_ok) {
            try {
                months->addValue(row_tokens[0]);
                towns->addValue(row_tokens[1]);
                flatTypes->addValue(row_tokens[2]);
                blocks->addValue(row_tokens[3]);
                streetNames->addValue(row_tokens[4]);
                storeyRanges->addValue(row_tokens[5]);
                try { floorAreas->addValue(std::stod(row_tokens[6])); } catch (...) { throw std::runtime_error("Invalid floor area: " + row_tokens[6]); }
                flatModels->addValue(row_tokens[7]);
                try { leaseCommenceDates->addValue(std::stoi(row_tokens[8])); } catch (...) { throw std::runtime_error("Invalid lease date: " + row_tokens[8]); }
                try { resalePrices->addValue(std::stod(row_tokens[9])); } catch (...) { throw std::runtime_error("Invalid resale price: " + row_tokens[9]); }

                rowCount++;
            } catch (const std::runtime_error& e) {
                 std::cerr << "Error processing CSV row: " << line << " | Reason: " << e.what() << std::endl;
                 row_ok = false;
            } catch (const std::invalid_argument& e) {
                 std::cerr << "Error converting value in CSV row: " << line << " | Reason: " << e.what() << std::endl;
                 row_ok = false;
            } catch (const std::out_of_range& e) {
                 std::cerr << "Error converting value (out of range) in CSV row: " << line << " | Reason: " << e.what() << std::endl;
                 row_ok = false;
            }
        }
    } 

    std::cout << "Loaded " << rowCount << " records from CSV." << std::endl;
}


// Save all columns to disk
void ColumnStore::saveToDisk() {
    std::cout << "Saving columns to disk in folder: " << dataFolderPath << " ..." << std::endl;

    try {
        if (!fs::exists(dataFolderPath)) {
            if (fs::create_directories(dataFolderPath)) { // Creates parent dirs too if needed
                std::cout << "Created data directory: " << dataFolderPath << std::endl;
            } else {
                std::cerr << "Error: Could not create data directory: " << dataFolderPath << std::endl;
                return;
            }
        } else if (!fs::is_directory(dataFolderPath)) {
             std::cerr << "Error: Path exists but is not a directory: " << dataFolderPath << std::endl;
             return;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error accessing/creating directory " << dataFolderPath << ": " << e.what() << std::endl;
        return;
    }

    months->storeToDisk();
    towns->storeToDisk();
    flatTypes->storeToDisk();
    blocks->storeToDisk();
    streetNames->storeToDisk();
    storeyRanges->storeToDisk();
    floorAreas->storeToDisk();
    flatModels->storeToDisk();
    leaseCommenceDates->storeToDisk();
    resalePrices->storeToDisk();

    // Save row count
    std::string countFilePath = buildFullPath("rowCount.dat");
    std::ofstream countFile(countFilePath, std::ios::binary | std::ios::trunc);
     if (!countFile) {
        std::cerr << "Error: Could not open row count file for writing: " << countFilePath << std::endl;
     } else {
        countFile.write(reinterpret_cast<const char*>(&rowCount), sizeof(size_t));
        if (!countFile) {
            std::cerr << "Error: Failed writing to row count file: " << countFilePath << std::endl;
        }
        countFile.close(); 
    }


    std::cout << "Data saved to disk." << std::endl;
}

// Load all columns from disk
void ColumnStore::loadFromDisk() {
    std::cout << "Loading columns from disk in folder: " << dataFolderPath << " ..." << std::endl;
    rowCount = 0;

    // Load row count first
    std::string countFilePath = buildFullPath("rowCount.dat");
    std::ifstream countFile(countFilePath, std::ios::binary);
    if (!countFile) {

    } else {
        countFile.read(reinterpret_cast<char*>(&rowCount), sizeof(size_t));
         if (!countFile || countFile.gcount() != sizeof(size_t)) {
             std::cerr << "Error: Failed to read row count from file: " << countFilePath << ". Data might be incomplete." << std::endl;
             rowCount = 0; // Reset on error
         }
        countFile.close();
    }

    months->loadFromDisk();
    towns->loadFromDisk();
    flatTypes->loadFromDisk();
    blocks->loadFromDisk();
    streetNames->loadFromDisk();
    storeyRanges->loadFromDisk();
    floorAreas->loadFromDisk();
    flatModels->loadFromDisk();
    leaseCommenceDates->loadFromDisk();
    resalePrices->loadFromDisk();


    size_t expectedSize = rowCount;
    bool consistent = true;
    std::vector<ColumnBase*> all_columns = {
        months.get(), towns.get(), flatTypes.get(), blocks.get(),
        streetNames.get(), storeyRanges.get(), floorAreas.get(),
        flatModels.get(), leaseCommenceDates.get(), resalePrices.get()
    };

    for (const auto& col : all_columns) {
        if (col && col->size() != expectedSize) { 
            std::cerr << "Warning: Inconsistent column size after loading. File: "
                      << col->getFileName() << " has size " << col->size()
                      << ", expected " << expectedSize << "." << std::endl;
            consistent = false;
        }
    }


    if (!consistent) {
        std::cerr << "Data loaded from disk might be inconsistent." << std::endl;
    } else if (rowCount > 0 || fs::exists(countFilePath)) { 
         std::cout << "Data loaded from disk. Row count: " << rowCount << std::endl;
    } else {
         std::cout << "No existing data found on disk." << std::endl;
    }
}

// Get number of rows
size_t ColumnStore::getRowCount() const {
    return rowCount;
}