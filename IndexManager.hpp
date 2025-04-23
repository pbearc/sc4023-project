// IndexManager.hpp
#pragma once

#include <vector>
#include <algorithm>
#include <utility>
#include <filesystem>
#include <string>
#include "Interval.h"      
#include "BPlusTree.hpp"    // your templated BPlusTree
#include "ColumnStore.h"
#include "Constants.h"

// Aliases for each of your per‐column trees:
using MonthTree       = BPlusTree<std::string, n_string>;
using TownTree        = BPlusTree<std::string, n_string>;
using FlatTypeTree    = BPlusTree<std::string, n_string>;
using BlockTree       = BPlusTree<std::string, n_string>;
using StreetTree      = BPlusTree<std::string, n_string>;
using StoreyTree      = BPlusTree<std::string, n_string>;
using FloorAreaTree   = BPlusTree<double, n_double>;
using ModelTree       = BPlusTree<std::string, n_string>;
using LeaseDateTree   = BPlusTree<int, n_int>;
using PriceTree       = BPlusTree<double, n_double>;

class IndexManager {
public:
    explicit IndexManager(const std::string &dir = "bptree")
        : _dir(dir)
        , _dirCreated( (std::filesystem::create_directories(_dir), true) )
        , monthTree(dir + "/month.idx")
        , townTree(dir + "/town.idx")
        , flatTypeTree(dir + "/flat_type.idx")
        , blockTree(dir + "/block.idx")
        , streetTree(dir + "/street_name.idx")
        , storeyTree(dir + "/storey_range.idx")
        , floorAreaTree(dir + "/floor_area.idx")
        , modelTree(dir + "/flat_model.idx")
        , leaseDateTree(dir + "/lease_commence_date.idx")
        , priceTree(dir + "/resale_price.idx")
        {}

    void buildIndexes(const ColumnStore &cs) {
        size_t rowCount = cs.getRowCount();

        // Shortcut: if there’s no data, nothing to do
        if (rowCount == 0) return;

        // Grab const references to each column’s data vector
        const auto &months      = cs.getMonths()->getData();
        const auto &towns       = cs.getTowns()->getData();
        const auto &flatTypes   = cs.getFlatTypes()->getData();
        const auto &blocks      = cs.getBlocks()->getData();
        const auto &streets     = cs.getStreetNames()->getData();
        const auto &storeys     = cs.getStoreyRanges()->getData();
        const auto &areas       = cs.getFloorAreas()->getData();
        const auto &models      = cs.getFlatModels()->getData();
        const auto &leases      = cs.getLeaseCommenceDates()->getData();
        const auto &prices      = cs.getResalePrices()->getData();

        for (size_t i = 0; i < rowCount; i++) {
            monthTree.insert(months[i], i);
            townTree.insert(towns[i], i);
            flatTypeTree.insert(flatTypes[i], i);
            blockTree.insert(blocks[i], i);
            streetTree.insert(streets[i], i);
            storeyTree.insert(storeys[i], i);
            floorAreaTree.insert(areas[i], i);
            modelTree.insert(models[i], i);
            leaseDateTree.insert(leases[i], i);
            priceTree.insert(prices[i], i);

            if ((i % 50) == 0) 
            {
                std::cout << "Indexed " << i << " / " << rowCount << " (<2min)\r" << std::flush;
            }
            if (i == (rowCount - 1))
            {
                //should be 222834
                std::cout << "\nIndex build complete for " << i+1 << " rows.\n";
            }
        }
    }

    // Multi‐attribute search. Each param defaults to {} → “no filter → all records.”
    std::vector<int> searchAll(
        const std::vector<Interval<std::string>>&    monthIVs   = {},
        const std::vector<Interval<std::string>>&  townIVs      = {},
        const std::vector<Interval<std::string>>&  flatTypeIVs  = {},
        const std::vector<Interval<std::string>>&  blockIVs     = {},
        const std::vector<Interval<std::string>>&  streetIVs    = {},
        const std::vector<Interval<std::string>>&  storeyIVs    = {},
        const std::vector<Interval<double>>&       floorAreaIVs = {},
        const std::vector<Interval<std::string>>&  modelIVs     = {},
        const std::vector<Interval<int>>&          leaseDateIVs = {},
        const std::vector<Interval<double>>&       priceIVs     = {}
    ) {
        // 1) Get the per‐column result lists
        auto mRes  = monthTree.    searchIntervals(monthIVs);
        std::cout << "Month filter returned "       << mRes.size() << " IDs\n";
        auto tRes  = townTree.     searchIntervals(townIVs);
        std::cout << "Town filter returned "        << tRes.size() << " IDs\n";
        auto ftRes = flatTypeTree. searchIntervals(flatTypeIVs);
        std::cout << "FlatType filter returned "    << ftRes.size() << " IDs\n";
        auto bRes  = blockTree.    searchIntervals(blockIVs);
        std::cout << "Block filter returned "       << bRes.size() << " IDs\n";
        auto sRes  = streetTree.   searchIntervals(streetIVs);
        std::cout << "StreetName filter returned "  << sRes.size() << " IDs\n";
        auto srRes = storeyTree.   searchIntervals(storeyIVs);
        std::cout << "StoreyRange filter returned " << srRes.size() << " IDs\n";
        auto faRes = floorAreaTree.searchIntervals(floorAreaIVs);
        std::cout << "FloorArea filter returned "   << faRes.size() << " IDs\n";
        auto moRes = modelTree.    searchIntervals(modelIVs);
        std::cout << "FlatModel filter returned "   << moRes.size() << " IDs\n";
        auto ldRes = leaseDateTree.searchIntervals(leaseDateIVs);
        std::cout << "LeaseDate filter returned "   << ldRes.size() << " IDs\n";
        auto pRes  = priceTree.    searchIntervals(priceIVs);
        std::cout << "ResalePrice filter returned " << pRes.size() << " IDs\n";

        // 2) Put them into a vector for easy looping
        std::vector<std::vector<int>> lists = {
            std::move(mRes), std::move(tRes), std::move(ftRes), std::move(bRes),
            std::move(sRes), std::move(srRes), std::move(faRes),
            std::move(moRes), std::move(ldRes), std::move(pRes)
        };

        // 3) Intersect them all
        return intersectAll(lists);
    }

private:
    // Efficient k‐way intersection of sorted, unique integer lists
    static std::vector<int> intersectAll(
        const std::vector<std::vector<int>>& lists
    ) {
        // Start with the first non‐empty list; if all empty, return {}
        std::vector<int> result;
        bool first = true;
        for (auto const& lst : lists) {
            if (first) {
                result = lst;           // copy first list
                first = false;
            } else {
                result = intersectTwo(result, lst);
            }
            if (result.empty())        // early out if no common elements
                break;
        }
        return result;
    }

    // Intersect two sorted unique vectors in linear time
    static std::vector<int> intersectTwo(
        const std::vector<int>& a,
        const std::vector<int>& b
    ) {
        std::vector<int> out;
        out.reserve(std::min(a.size(), b.size()));
        size_t i = 0, j = 0;
        while (i < a.size() && j < b.size()) {
            if      (a[i] < b[j]) ++i;
            else if (b[j] < a[i]) ++j;
            else {                    // a[i] == b[j]
                out.push_back(a[i]);
                ++i; ++j;
            }
        }
        return out;
    }

    // Your per‑attribute trees:
    std::string _dir;
    bool _dirCreated;
    MonthTree     monthTree;
    TownTree      townTree;
    FlatTypeTree  flatTypeTree;
    BlockTree     blockTree;
    StreetTree    streetTree;
    StoreyTree    storeyTree;
    FloorAreaTree floorAreaTree;
    ModelTree     modelTree;
    LeaseDateTree leaseDateTree;
    PriceTree     priceTree;
};



