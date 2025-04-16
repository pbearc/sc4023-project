## data storage part

Each column (month, town, price etc.) is stored separately in binary files

Example files: col_months.dat, col_towns.dat, col_resalePrices.dat

Compile the program with

```
g++ -std=c++17 main.cpp columnstore.cpp -o column_app -lstdc++fs
```

Run the compiled program

```
./column_app
```
