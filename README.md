# Cluster join implementation example

## Description

Get the results of the following query processing the provided CSV files in parallel.

```sql
select
    `Donor State`,
    sum(`Donation Amount`)
from donors, donations
where donations.`Donor ID` = donors.`Donor ID`
group by `Donor State`
```

## Runing the app

1. Install [Tarantool](https://www.tarantool.io/en/download/) from source or as a binary package.

2. Clone the app repository:
    
    ```
    git clone https://github.com/msiomkin/cube.git
    ```
    
3. Copy data files 'Donors.csv' and 'Donations.csv' to 'cube' directory

4. Run the app from 'cube' directory:
    
    ```
    ./processor.lua
    ```
    
    Optionally you can set nodes count (8 by default):
    
    ```
    CUBE_NODE_COUNT=4 ./processor.lua
    ```
