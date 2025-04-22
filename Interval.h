#pragma once
// Defines the four kinds of boundary inclusions
enum class IntervalType {
    ClosedClosed,   // [start,end]
    ClosedOpen,     // [start,end)
    OpenClosed,     // (start,end]
    OpenOpen,        // (start,end)
    UpToClosed,     // [,end]
    UpToOpen,       // [,end)
    FromClosed,     // [start,]
    FromOpen        // (start,]
};

// Holder for one interval on a Key type
template<typename Key>
struct Interval {
    IntervalType type;
    Key          start;
    Key          end;
};
