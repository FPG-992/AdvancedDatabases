## Standard Operating Procedure for Running the App

### Requirements
- Docker installed and running
- Tested on macOS

### Steps
1. Git clone the repo
2. CD into the repo
3. Place a folder called `project` inside the root directory of the cloned repo with the data in the following structure:

```text
project_data
┣ LA_Crime_Data
┃ ┣ LA_Crime_Data_2010_2019.csv
┃ ┗ LA_Crime_Data_2020_2025.csv
┣ LA_Census_Blocks_2020.geojson
┣ LA_Census_Blocks_2020_fields.csv
┣ LA_Police_Stations.csv
┣ LA_income_2021.csv
┣ MO_codes.txt
┗ RE_codes.csv
