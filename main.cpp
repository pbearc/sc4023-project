#include "ColumnStore.h"
#include <iostream>
#include <chrono>
#include <string>
#include <filesystem>
#include <algorithm> 
#include "IndexManager.hpp"

namespace fs = std::filesystem;

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
    IndexManager idxMgr("bptree");
    idxMgr.buildIndexes(store); //takes about 2min
    
    std::cout << R"SQL(Example 1 Query:
    SELECT *
        FROM ResalePricesSingapore
        WHERE 
            Month BETWEEN '2015-09' AND '2015-10'
            AND Town = 'BUKIT PANJANG'
            AND FloorArea >= 80;
    )SQL" << std::endl;

    // 2) Construct intervals for:
    //    Month BETWEEN '2015-09' AND '2015-10'
    std::vector<Interval<std::string>> monthIVs;
    monthIVs.push_back({ IntervalType::ClosedClosed, "2015-09", "2015-10" });

    //    Town = 'JURONG WEST'
    std::vector<Interval<std::string>> townIVs;
    townIVs.push_back({ IntervalType::ClosedClosed, "BUKIT PANJANG", "BUKIT PANJANG" });

    //    Area >= 80
    std::vector<Interval<double>> areaIVs;
    areaIVs.push_back({ IntervalType::FromClosed, 80.0, 0.0});

    // 3) Run the multi‑attribute search
    auto recordIds = idxMgr.searchAll(
        monthIVs, townIVs,
        /*flatTypeIVs=*/{}, /*blockIVs=*/{}, /*streetIVs=*/{},
        /*storeyIVs=*/{}, areaIVs,
        /*modelIVs=*/{}, /*leaseDateIVs=*/{}, /*priceIVs=*/{}
    );

    // 4) Fetch and print the matching rows, rows is vector<pair<recordID, DataRow>>
    auto rows = store.fetchRows(recordIds);
    std::cout << "\nExample 1 Results (" << recordIds.size() << " rows):\n";
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

    // Example 2: multi‑interval filters on every column
    const char* example2_sql = R"SQL(
        SELECT *
        FROM ResalePricesSingapore
        WHERE
          (Month <= '2017-12' OR Month = '2018-02')
          AND Town IN ('JURONG WEST', 'CLEMENTI')
          AND FlatType IN ('4 ROOM', '5 ROOM')
          AND (FloorArea >= 80 OR FloorArea < 70)
          AND FlatModel IN ('STANDARD', 'IMPROVED')
          AND ((LeaseCommenceDate >= 1990 AND LeaseCommenceDate < 1999)
               OR LeaseCommenceDate > 2000)
          AND ((ResalePrice > 300000 AND ResalePrice <= 400000)
               OR (ResalePrice > 500000 AND ResalePrice < 600000));
        )SQL";
    std::cout << "\nExample 2 SQL Query:\n" << example2_sql << std::endl;
    
    //  - Month in (≤2017‑12) or =2018‑02
    std::vector<Interval<std::string>> monthIVs2 = {
        { IntervalType::UpToClosed,   "",         "2017-12" },
        { IntervalType::ClosedClosed, "2018-02",  "2018-02" }
    };

    // Town ∈ {JURONG WEST, CLEMENTI}
    std::vector<Interval<std::string>> townIVs2 = {
        { IntervalType::ClosedClosed, "JURONG WEST", "JURONG WEST" },
        { IntervalType::ClosedClosed, "CLEMENTI",     "CLEMENTI"     }
    };

    // FlatType ∈ {4 ROOM, 5 ROOM}
    std::vector<Interval<std::string>> flatTypeIVs2 = {
        { IntervalType::ClosedClosed, "4 ROOM", "4 ROOM" },
        { IntervalType::ClosedClosed, "5 ROOM", "5 ROOM" }
    };

    // FloorArea ∈ [80, ∞) or (−∞, 70)
    std::vector<Interval<double>> areaIVs2 = {
        { IntervalType::FromClosed, 80.0,  0.0 },
        { IntervalType::UpToOpen,   0.0, 70.0 }
    };

    // FlatModel ∈ {"STANDARD", "IMPROVED"}
    std::vector<Interval<std::string>> modelIVs2 = {
        { IntervalType::ClosedClosed, "STANDARD", "STANDARD" },
        { IntervalType::ClosedClosed, "IMPROVED", "IMPROVED" }
    };

    // LeaseDate ∈ [1990,1999) or (2000, ∞)
    std::vector<Interval<int>> leaseDateIVs2 = {
        { IntervalType::ClosedOpen, 1990, 1999 },
        { IntervalType::FromOpen,   2000,    0 }
    };

    // ResalePrice ∈ (300000, 400000] or (500000, 600000)
    std::vector<Interval<double>> priceIVs2 = {
        { IntervalType::OpenClosed, 300000.0, 400000.0 },
        { IntervalType::OpenOpen,   500000.0, 600000.0 }
    };

    auto recs2 = idxMgr.searchAll(
        monthIVs2,
        townIVs2,
        flatTypeIVs2,
        /*blockIVs=*/{}, /*streetIVs=*/{}, /*storeyIVs=*/{},
        areaIVs2,
        modelIVs2,
        leaseDateIVs2,
        priceIVs2
    );

    std::cout << "\nExample 2 Results: " << recs2.size() << " rows\n";
    for (auto const &pr : store.fetchRows(recs2)) {
        const auto &r = pr.second;
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


    return 0;
}