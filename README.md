# Friends by Age Analysis using PySpark

## Overview
This repository contains a Spark-based Python script that analyzes the number of friends across different age groups from a CSV file. The script uses PySpark to process and analyze data efficiently on distributed systems.

## Features
- **Input Data**: A CSV file containing age and friends data.
- **Data Processing**: 
  - Parses the data to extract age and friends count.
  - Computes the total friends for each age group.
  - Calculates the average number of friends per age group.
  - Sorts the results by age.

## Script Overview
- `FriendsByAge.py`: The main script that processes the CSV file to analyze friends by age.
  - **Functionality**:
    - `parseData(data)`: Parses each line of the CSV to extract age and friends count.
    - `totalsByAge`: Calculates total friends and count for each age group using `reduceByKey`.
    - `averagesByAge`: Computes the average number of friends per age group.
    - Results are sorted by age and printed.

## Example Output
```
(20, 15.5)
(21, 18.3)
(22, 17.7)
...
```
