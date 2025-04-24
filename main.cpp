#include "ColumnStore.h"
#include <iostream>
#include <chrono>
#include <string>
#include <filesystem>
#include <algorithm> 
#include "IndexManager.hpp"
#include <fstream>

namespace fs = std::filesystem;

void writeResultToCSV(const std::string& filename,
    const std::string& queryCategory,
    int year,
    int month,
    const std::string& town,
    double result,
    bool writeHeader);

std::string formatYearMonth(int year, int month);
double sd_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows);
double min_result_per_sqm(std::vector<std::pair<int, ColumnStore::DataRow>> rows);
double min_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows);
double average_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows);

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

    if (store.getRowCount() > 0) { // should be 222834
        std::cout << "Total records available: " << store.getRowCount() << std::endl;

        // Print first few records as a sample
        std::cout << "\nSample data (first 5 records):" << std::endl;
        std::cout << "Month\tTown\tFlat Type\tFloor Area\tResale Price" << std::endl;
        std::cout << "----------------------------------------------------------------" << std::endl;

        size_t sampleSize = std::min(store.getRowCount(), static_cast<size_t>(5));
        for (size_t i = 0; i < sampleSize; i++) {
            // Access data using getData() and [] operator
            std::cout << store.getMonths()->getData()[i] << "\t"
                      << store.getTowns()->getData()[i] << "\t"
                      << store.getFlatTypes()->getData()[i] << "\t"
                      << store.getFloorAreas()->getData()[i] << "\t\t" 
                      << store.getResalePrices()->getData()[i] << std::endl;
        }

        std::cout << "\nColumn store is ready for querying." << std::endl;
        std::cout << "Use the column accessor methods (e.g., store.getTowns()->getData()[index]) to retrieve data." << std::endl;

    } else {
        std::cout << "No data loaded into the column store." << std::endl;
    }

    // 1) Build the B+ tree indexes
    std::cout << "Building the B+ Tree....." << std::endl;
    IndexManager idxMgr("bptree");
    idxMgr.buildIndexes(store); //takes about 2min
    
    // Query User Interface --> ask for query category and filters.
    if(store.getRowCount() > 0){
    int queryChoice = 0;
    int startYear = 0;
    int startMonth = 0;
    double calculated_result = 0;
    std::string queryCategory;
    std::string town;
    std::string outputFilename;
    bool writeHeader = true;
    bool askFilename = true;
    
    while (true){
        std::cout << "=== Query Interface ===\n";
    
        // Ask for query category with full validation
        while (true) {
            std::cout << "Select query category:\n";
            std::cout << "Input '1': AVG(Price)\n";
            std::cout << "Input '2': MIN(Price)\n";
            std::cout << "Input '3': SD(Price)\n";
            std::cout << "Input '4': MIN(Price_per_sqm)\n";
            std::cout << "Input '0': END QUERY\n";
            std::cout << "Enter choice (0-4): ";

            if (std::cin >> queryChoice) {
                if (queryChoice >= 0 && queryChoice <= 4) {
                    break;  // valid integer in range
                } else {
                    std::cout << "Invalid number. Please enter a number between 1 and 4.\n";
                }
            } else {
                // Clear the fail state and ignore invalid input
                std::cout << "Invalid input. Please enter a number.\n";
                std::cin.clear();
                std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
        }
        if(queryChoice == 0) break;

        switch (queryChoice) {
            case 1: queryCategory = "AVG(Price)"; break;
            case 2: queryCategory = "MIN(Price)"; break;
            case 3: queryCategory = "SD(Price)"; break;
            case 4: queryCategory = "MIN(Price_per_sqm)"; break;
        }
        std::cin.ignore(); // clear newline from input buffer

        // Ask for filter: start year with validation
        while (true) {
            std::cout << "Enter the start year (2014 - 2024):\n";
            if (std::cin >> startYear) {
                if (startYear >= 2014 && startYear <= 2024) {
                    break;  // valid integer in range
                } else {
                    std::cout << "Invalid number. Please enter a number between 2014 and 2024.\n";
                }
            } else {
                // Clear the fail state and ignore invalid input
                std::cout << "Invalid input. Please enter a number.\n";
                std::cin.clear();
                std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
        }
        std::cin.ignore(); // clear newline from input buffer

        // Ask for filter: start month with validation
        while (true) {
            std::cout << "Enter the start month (1 - 12):\n";
            if (std::cin >> startMonth) {
                if (startMonth >= 1 && startMonth <= 12) {
                    break;  // valid integer in range
                } else {
                    std::cout << "Invalid number. Please enter a number between 1 and 12.\n";
                }
            } else {
                // Clear the fail state and ignore invalid input
                std::cout << "Invalid input. Please enter a number.\n";
                std::cin.clear();
                std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
        }
        std::cin.ignore(); // clear newline from input buffer

        // Ask for filter: town
        std::cout << "Enter town (e.g.,BEDOK, BUKIT PANJANG, CLEMENTI, CHOA CHU KANG, HOUGANG, JURONG WEST, PASIR RIS, TAMPINES, WOODLANDS, YISHUN):\n";
        std::getline(std::cin, town);
    
        // Ask for output filename
        if (askFilename){
        std::cout << "Enter output csv filename to save result (e.g., ScanResult_<Matric_Number>.csv):\n ";
        std::getline(std::cin, outputFilename);
        askFilename = false;
        }

        // Display collected inputs (for debug/demo)      
        std::cout << "\nYou entered:\n";
        std::cout << "Query Category: " << queryCategory << '\n';
        std::cout << "Start Year: " << startYear << '\n';
        std::cout << "Start Month: " << startMonth << '\n';
        std::cout << "Town: " << town << '\n';
        std::cout << "Output Filename: " << outputFilename << '\n';
        std::cout << "Processing Query....." << std::endl;

        //Process Query
        // 1) Construct intervals for:
        //    Month BETWEEN '2022-07' AND '2022-08'
        std::vector<Interval<std::string>> monthIVs;
        if(startMonth<12){
            monthIVs.push_back({ IntervalType::ClosedClosed, formatYearMonth(startYear,startMonth), formatYearMonth(startYear,startMonth+1)});
        }else{
            monthIVs.push_back({ IntervalType::ClosedClosed, formatYearMonth(startYear,startMonth), formatYearMonth(startYear+1,1)});
        }

        //    Town = 'YISHUN'
        std::vector<Interval<std::string>> townIVs;
        townIVs.push_back({ IntervalType::ClosedClosed, town, town });

        //    Area >= 80
        std::vector<Interval<double>> areaIVs;
        areaIVs.push_back({ IntervalType::FromClosed, 80.0, 0.0});

        // 2) Run the multi‑attribute search
        auto recordIds = idxMgr.searchAll(
            monthIVs, townIVs,
            /*flatTypeIVs=*/{}, /*blockIVs=*/{}, /*streetIVs=*/{},
            /*storeyIVs=*/{}, areaIVs,
            /*modelIVs=*/{}, /*leaseDateIVs=*/{}, /*priceIVs=*/{}
        );

        // 3) Fetch and print the matching rows, rows is vector<pair<recordID, DataRow>>
        auto rows = store.fetchRows(recordIds);
        std::cout << "\nQuery Results (" << recordIds.size() << " rows):\n";
        for (auto const& pr : rows) {
            const auto& r = pr.second;
            const auto& idx = pr.first;
            std::cout
                << idx       << ": "
                << r.month       << ", "
                << r.town        << ", "
                << r.flatType    << ", "
                << r.block    << ", "
                << r.streetName    << ", "
                << r.storeyRange    << ", "
                << r.floorArea   << ", "
                << r.flatModel   << ", "
                << r.leaseDate   << ", "
                << r.resalePrice << "\n";
        }

        // 5)Get result
        switch (queryChoice) {
            case 1: calculated_result = average_result(rows); break;
            case 2: calculated_result = min_result(rows); break;
            case 3: calculated_result = sd_result(rows); break;
            case 4: calculated_result = min_result_per_sqm(rows); break;
        }

        std::cout << "Calculated Result " << queryCategory << ": " << calculated_result << '\n';

        writeResultToCSV(outputFilename, queryCategory,startYear,startMonth,town,calculated_result,writeHeader);
        writeHeader = false;
    }
    }else {
        std::cout << "No data loaded into the column store." << std::endl;
    }
    return 0;
}

void writeResultToCSV(const std::string& filename,
    const std::string& queryCategory,
    int year,
    int month,
    const std::string& town,
    double result,
    bool writeHeader) {
    std::ofstream csvFile;

    // Open in append mode
    csvFile.open(filename, std::ios::app);

    if (!csvFile.is_open()) {
    std::cerr << "Failed to open " << filename << std::endl;
    return;
    }

    // Write header if needed
    if(writeHeader){
    csvFile << "Year,Month,town,Category,Value\n";
    }
    // Write data
    csvFile << year << ","
    << std::setw(2) << std::setfill('0') << month << ","
    << town << ","
    << queryCategory << ","
    << std::fixed << std::setprecision(2) << result << "\n";

    csvFile.close();
}

std::string formatYearMonth(int year, int month) {
    std::ostringstream oss;
    oss << std::setw(4) << std::setfill('0') << year << '-'
        << std::setw(2) << std::setfill('0') << month;
    return oss.str();
}

double average_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows){
    double sum = 0.00;
    int count = 0;
    for (auto const& pr : rows) {
        const auto& r = pr.second;
        sum = sum + r.resalePrice;
        count++;
    }
    return sum/count;
}

double min_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows){
    double min = 999999999999;
    for (auto const& pr : rows) {
        const auto& r = pr.second;
        if(min>= r.resalePrice){
            min = r.resalePrice;
        }
    }
    return min;
}

double min_result_per_sqm(std::vector<std::pair<int, ColumnStore::DataRow>> rows){
    double min = 999999999999;
    for (auto const& pr : rows) {
        const auto& r = pr.second;
        if(min>= r.resalePrice / r.floorArea){
            min = r.resalePrice / r.floorArea;
        }
    }
    return min;
}

double sd_result(std::vector<std::pair<int, ColumnStore::DataRow>> rows){
    double sum = 0.00;
    double squared_sum = 0.00;
    int count = 0;
    for (auto const& pr : rows) {
        const auto& r = pr.second;
        sum = sum + r.resalePrice;
        squared_sum = squared_sum + r.resalePrice * r.resalePrice;
        count++;
    }

    double mean = sum/count;
    double variance = squared_sum/count - mean * mean;
    return std::sqrt(variance);
}