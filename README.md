Standard Operating Procedure for running the app.
Requirements: 
- Docker Installed with resource allocation of 10 cpu cores and 16GB Ram
- macOS arm64 
1. Git clone the repo
2. CD into the repo
3. Place a folder called project_data inside the root directory of the cloned repo with the data in the following structure:
    project_data
    - LA_Crime_Data
    - - LA_Crime_Data_2010_2019.csv
    - - LA_Crime_Data_2020_2025.csv
    - LA_Census_Blocks_2020.geojson
    - LA_Census_Blocks_2020_fields.csv
    - LA_Police_Stations.csv
    - LA_income_2021.csv
    - MO_codes.txt
    - RE_codes.csv
4. Create the executable to run the repo with chmod +x ./run_git.sh
5. Run the executable ./run_git.sh
6. Follow the directions on the command line to run the Queries, one each time or all together.
