#include "ColumnStore.h"
#include <iostream>
#include <chrono>
#include <string>
#include <filesystem> 
#include <algorithm>

namespace fs = std::filesystem; // Alias

int main() {
   
    const std::string dataFolder = "hdb_data_store"; 
    const std::string csvFile = "ResalePricesSingapore.csv";
    const std::string checkFilename = "col_months.dat"; 


    std::cout << "Using data folder: " << dataFolder << std::endl;
    ColumnStore store(dataFolder); 

    fs::path checkFilePath = fs::path(dataFolder) / checkFilename;
    bool dataExistsOnDisk = fs::exists(checkFilePath);

    auto startTime = std::chrono::high_resolution_clock::now();

    if (dataExistsOnDisk) {
        std::cout << "Found existing column data files in '" << dataFolder
                  << "' (checked: " << checkFilename << "). Loading data from disk..." << std::endl;
        store.loadFromDisk();

    } else {
        std::cout << "No existing column data found in '" << dataFolder
                  << "' (checked: " << checkFilename << "). Processing CSV file '" << csvFile << "'..." << std::endl;
        store.loadFromCSV(csvFile);

        // Only save if loading CSV was successful (produced rows)
        if (store.getRowCount() > 0) {
             std::cout << "Saving processed data to disk for future use..." << std::endl;
             store.saveToDisk();
        } else {
             std::cerr << "Warning: No records loaded from CSV. Nothing to save." << std::endl;
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

    std::cout << "Operation completed in " << duration << " milliseconds." << std::endl;

    if (store.getRowCount() > 0) {
        std::cout << "Total records available: " << store.getRowCount() << std::endl;

        // Print first few records as a sample
        std::cout << "\nSample data (first 5 records):" << std::endl;
        std::cout << "Month\tTown\tFlat Type\tFloor Area\tResale Price" << std::endl;
        std::cout << "----------------------------------------------------------------" << std::endl; // Adjusted separator length

        size_t sampleSize = std::min(store.getRowCount(), static_cast<size_t>(5));
        for (size_t i = 0; i < sampleSize; i++) {
            try {
                std::cout << store.getMonths()->getValue(i) << "\t"
                          << store.getTowns()->getValue(i) << "\t"
                          << store.getFlatTypes()->getValue(i) << "\t"
                          << store.getFloorAreas()->getValue(i) << "\t\t" // Extra tab maybe needed
                          << store.getResalePrices()->getValue(i) << std::endl;
            } catch (const std::out_of_range& e) {
                 std::cerr << "Error getting sample data at index " << i << ": " << e.what() << std::endl;
                 break;
            }
        }

        std::cout << "\nColumn store is ready for querying." << std::endl;
        std::cout << "Use the column accessor methods (e.g., store.getTowns()->getValue(index)) to retrieve data." << std::endl;
    } else {
         std::cout << "No data loaded into the column store." << std::endl;
    }

    return 0;
}