#include "ColumnStore.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <algorithm>
#include <vector>
#include <cstring>
#include <cmath>

namespace fs = std::filesystem;

// Template specialization for storing numeric types (int)
template <>
void Column<int>::storeToDisk() {
    std::ofstream file(fullFilePath, std::ios::binary | std::ios::trunc);
    if (!file) {
        std::cerr << "Error: Could not open file for writing: " << fullFilePath << std::endl;
        return;
    }

    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(size_t)); // Write total count

    if (count > 0) {
        const size_t valuesPerBlock = BLOCK_SIZE / sizeof(int);
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / valuesPerBlock));
        
        for (size_t i = 0; i < totalBlocks; ++i) {
            size_t startIdx = i * valuesPerBlock;
            size_t numValues = std::min(valuesPerBlock, count - startIdx);
            
            char buffer[BLOCK_SIZE] = {0};
            
            if (numValues > 0) {
                memcpy(buffer, &data[startIdx], numValues * sizeof(int));
            }
            
            file.write(buffer, BLOCK_SIZE);
        }
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
        const size_t valuesPerBlock = BLOCK_SIZE / sizeof(double);
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / valuesPerBlock));
        
        for (size_t i = 0; i < totalBlocks; ++i) {
            size_t startIdx = i * valuesPerBlock;
            size_t numValues = std::min(valuesPerBlock, count - startIdx);
            
            char buffer[BLOCK_SIZE] = {0};
            
            if (numValues > 0) {
                memcpy(buffer, &data[startIdx], numValues * sizeof(double));
            }
            
            file.write(buffer, BLOCK_SIZE);
        }
    }
}

// Template specialization for storing string columns
template <>
void Column<std::string>::storeToDisk() {
    std::ofstream file(fullFilePath, std::ios::binary | std::ios::trunc);
    if (!file) {
        std::cerr << "Error: Could not open file for writing: " << fullFilePath << std::endl;
        return;
    }

    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(size_t)); // Write total count

    if (count > 0) {
        // Write the data in blocks
        const size_t stringsPerBlock = BLOCK_SIZE / FIXED_STRING_LEN;
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / stringsPerBlock));
        
        for (size_t i = 0; i < totalBlocks; ++i) {
            size_t startIdx = i * stringsPerBlock;
            size_t numStrings = std::min(stringsPerBlock, count - startIdx);
            
            char buffer[BLOCK_SIZE] = {0};
            
            for (size_t j = 0; j < numStrings; ++j) {
                const std::string& str = data[startIdx + j];
                size_t copyLen = std::min(FIXED_STRING_LEN - 1, str.size());
                memcpy(buffer + j * FIXED_STRING_LEN, str.c_str(), copyLen);
            }
            
            file.write(buffer, BLOCK_SIZE);
        }
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
    if (!file || file.gcount() != sizeof(size_t)) {
        return;
    }

    if (count > 0) {
        data.resize(count);
        const size_t valuesPerBlock = BLOCK_SIZE / sizeof(int);
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / valuesPerBlock));
        
        for (size_t i = 0; i < totalBlocks && file; ++i) {
            char buffer[BLOCK_SIZE];
            file.read(buffer, BLOCK_SIZE);
            
            if (!file && !file.eof()) {
                data.clear();
                return;
            }
            
            size_t bytesRead = file.gcount();
            if (bytesRead == 0) {
                break;
            }
            
            size_t startIdx = i * valuesPerBlock;
            size_t numValues = std::min(valuesPerBlock, count - startIdx);
            size_t valuesToCopy = std::min(numValues, bytesRead / sizeof(int));
            
            if (valuesToCopy > 0) {
                memcpy(&data[startIdx], buffer, valuesToCopy * sizeof(int));
            }
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
    if (!file || file.gcount() != sizeof(size_t)) {
        return;
    }

    if (count > 0) {
        data.resize(count);
        const size_t valuesPerBlock = BLOCK_SIZE / sizeof(double);
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / valuesPerBlock));
        
        for (size_t i = 0; i < totalBlocks && file; ++i) {
            char buffer[BLOCK_SIZE];
            file.read(buffer, BLOCK_SIZE);
            
            if (!file && !file.eof()) {
                data.clear();
                return;
            }
            
            size_t bytesRead = file.gcount();
            if (bytesRead == 0) {
                break; // End of file
            }
            
            size_t startIdx = i * valuesPerBlock;
            size_t numValues = std::min(valuesPerBlock, count - startIdx);
            size_t valuesToCopy = std::min(numValues, bytesRead / sizeof(double));
            
            if (valuesToCopy > 0) {
                memcpy(&data[startIdx], buffer, valuesToCopy * sizeof(double));
            }
        }
    }
}

// Template specialization for loading string columns
template <>
void Column<std::string>::loadFromDisk() {
    data.clear();
    std::ifstream file(fullFilePath, std::ios::binary);
    if (!file) {
        return;
    }

    size_t count = 0;
    file.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (!file || file.gcount() != sizeof(size_t)) {
        return;
    }

    if (count > 0) {
        data.resize(count);
        const size_t stringsPerBlock = BLOCK_SIZE / FIXED_STRING_LEN;
        const size_t totalBlocks = static_cast<size_t>(std::ceil(static_cast<double>(count) / stringsPerBlock));
        
        for (size_t i = 0; i < totalBlocks && file; ++i) {
            char buffer[BLOCK_SIZE];
            file.read(buffer, BLOCK_SIZE);
            
            if (!file && !file.eof()) {
                data.clear();
                return;
            }
            
            size_t bytesRead = file.gcount();
            if (bytesRead == 0) {
                break; // End of file
            }
            
            size_t startIdx = i * stringsPerBlock;
            size_t numStrings = std::min(stringsPerBlock, count - startIdx);
            size_t stringsToCopy = std::min(numStrings, bytesRead / FIXED_STRING_LEN);
            
            for (size_t j = 0; j < stringsToCopy; ++j) {
                char* strPtr = buffer + j * FIXED_STRING_LEN;
                size_t strLen = strnlen(strPtr, FIXED_STRING_LEN);
                data[startIdx + j].assign(strPtr, strLen);
            }
        }
    }
}

// build full path using <filesystem>
std::string ColumnStore::buildFullPath(const std::string& filename) const {
    fs::path dirPath(dataFolderPath);
    fs::path filePath = dirPath / filename;
    return filePath.string();
}

// Constructor implementation
ColumnStore::ColumnStore(const std::string& folderPath)
    : dataFolderPath(folderPath), rowCount(0)
{
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
    if (!file) {
        std::cerr << "Error: Could not open CSV file: " << csvFilename << std::endl;
        return;
    }

    // Clear existing data
    months->clear();
    towns->clear();
    flatTypes->clear();
    blocks->clear();
    streetNames->clear();
    storeyRanges->clear();
    floorAreas->clear();
    flatModels->clear();
    leaseCommenceDates->clear();
    resalePrices->clear();
    rowCount = 0;

    std::string line;
    
    if (!std::getline(file, line)) {
        std::cerr << "Error: Could not read header line or file is empty: " << csvFilename << std::endl;
        return;
    }

    while (std::getline(file, line)) {
        if (line.empty() || line.find_first_not_of(" \t\r\n") == std::string::npos) {
            continue; 
        }

        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(ss, token, ',')) {
            token.erase(0, token.find_first_not_of(" \t\r\n"));
            token.erase(token.find_last_not_of(" \t\r\n") + 1);
            tokens.push_back(token);
        }

        // Check if we have enough columns
        if (tokens.size() < 10) {
            std::cerr << "Warning: Skipping row with insufficient columns: " << line << std::endl;
            continue;
        }

        try {
            // Parse values
            std::string month = tokens[0];
            std::string town = tokens[1];
            std::string flatType = tokens[2];
            std::string block = tokens[3];
            std::string streetName = tokens[4];
            std::string storeyRange = tokens[5];
            double floorArea = std::stod(tokens[6]);
            std::string flatModel = tokens[7];
            int leaseDate = std::stoi(tokens[8]);
            double resalePrice = std::stod(tokens[9]);

            // Add to columns
            months->addValue(month);
            towns->addValue(town);
            flatTypes->addValue(flatType);
            blocks->addValue(block);
            streetNames->addValue(streetName);
            storeyRanges->addValue(storeyRange);
            floorAreas->addValue(floorArea);
            flatModels->addValue(flatModel);
            leaseCommenceDates->addValue(leaseDate);
            resalePrices->addValue(resalePrice);

            rowCount++;
        }
        catch (const std::exception& e) {
            std::cerr << "Error processing row: " << line << " | Reason: " << e.what() << std::endl;
        }
    }

    std::cout << "Successfully loaded " << rowCount << " records from CSV." << std::endl;
}

// Save all columns to disk
void ColumnStore::saveToDisk() {
    std::cout << "Saving columns to disk in folder: " << dataFolderPath << " ..." << std::endl;

    try {
        if (!fs::exists(dataFolderPath)) {
            if (!fs::create_directories(dataFolderPath)) {
                std::cerr << "Error: Could not create data directory: " << dataFolderPath << std::endl;
                return;
            }
        }
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
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

    std::string countFilePath = buildFullPath("rowCount.dat");
    std::ofstream countFile(countFilePath, std::ios::binary | std::ios::trunc);
    if (countFile) {
        countFile.write(reinterpret_cast<const char*>(&rowCount), sizeof(size_t));
    }

    std::cout << "Data saving process complete." << std::endl;
}

// Load all columns from disk
void ColumnStore::loadFromDisk() {
    std::cout << "Loading columns from disk in folder: " << dataFolderPath << " ..." << std::endl;

    size_t storedRowCount = 0;
    std::string countFilePath = buildFullPath("rowCount.dat");
    std::ifstream countFile(countFilePath, std::ios::binary);
    if (countFile) {
        countFile.read(reinterpret_cast<char*>(&storedRowCount), sizeof(size_t));
        if (countFile.gcount() == sizeof(size_t)) {
            std::cout << "Loaded row count from file: " << storedRowCount << std::endl;
        }
    }

    months->clear();
    towns->clear();
    flatTypes->clear();
    blocks->clear();
    streetNames->clear();
    storeyRanges->clear();
    floorAreas->clear();
    flatModels->clear();
    leaseCommenceDates->clear();
    resalePrices->clear();
    rowCount = 0;

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

    size_t monthsSize = months->size();
    if (monthsSize > 0 &&
        monthsSize == towns->size() &&
        monthsSize == flatTypes->size() &&
        monthsSize == blocks->size() &&
        monthsSize == streetNames->size() &&
        monthsSize == storeyRanges->size() &&
        monthsSize == floorAreas->size() &&
        monthsSize == flatModels->size() &&
        monthsSize == leaseCommenceDates->size() &&
        monthsSize == resalePrices->size()) {
        
        rowCount = monthsSize;
        std::cout << "Data loaded successfully. Row count: " << rowCount << std::endl;
    }
    else if (monthsSize > 0) {
        std::cerr << "Data loaded from disk is inconsistent. Using the largest consistent subset." << std::endl;
        
        // Find the largest consistent subset
        std::vector<size_t> sizes = {
            months->size(), towns->size(), flatTypes->size(), blocks->size(),
            streetNames->size(), storeyRanges->size(), floorAreas->size(),
            flatModels->size(), leaseCommenceDates->size(), resalePrices->size()
        };
        
        // Find the most common size
        std::sort(sizes.begin(), sizes.end());
        size_t mostCommonSize = sizes[sizes.size() / 2]; // Median size
        
        if (mostCommonSize > 0) {
            rowCount = mostCommonSize;
            std::cout << "Using row count: " << rowCount << std::endl;
        }
        else {
            std::cerr << "Unable to determine consistent row count. No data loaded." << std::endl;
        }
    }
}

// Get number of rows
size_t ColumnStore::getRowCount() const {
    return rowCount;
}