File Structure in order for the scripts, code to run, folder structure for the data
📦AdvancedDatabases
 ┣ 📂.git
 ┃ ┣ 📂hooks
 ┃ ┃ ┣ 📜applypatch-msg.sample
 ┃ ┃ ┣ 📜commit-msg.sample
 ┃ ┃ ┣ 📜fsmonitor-watchman.sample
 ┃ ┃ ┣ 📜post-update.sample
 ┃ ┃ ┣ 📜pre-applypatch.sample
 ┃ ┃ ┣ 📜pre-commit.sample
 ┃ ┃ ┣ 📜pre-merge-commit.sample
 ┃ ┃ ┣ 📜pre-push.sample
 ┃ ┃ ┣ 📜pre-rebase.sample
 ┃ ┃ ┣ 📜pre-receive.sample
 ┃ ┃ ┣ 📜prepare-commit-msg.sample
 ┃ ┃ ┣ 📜push-to-checkout.sample
 ┃ ┃ ┣ 📜sendemail-validate.sample
 ┃ ┃ ┗ 📜update.sample
 ┃ ┣ 📂info
 ┃ ┃ ┗ 📜exclude
 ┃ ┣ 📂logs
 ┃ ┃ ┣ 📂refs
 ┃ ┃ ┃ ┣ 📂heads
 ┃ ┃ ┃ ┃ ┗ 📜main
 ┃ ┃ ┃ ┗ 📂remotes
 ┃ ┃ ┃ ┃ ┗ 📂origin
 ┃ ┃ ┃ ┃ ┃ ┗ 📜HEAD
 ┃ ┃ ┗ 📜HEAD
 ┃ ┣ 📂objects
 ┃ ┃ ┣ 📂info
 ┃ ┃ ┗ 📂pack
 ┃ ┃ ┃ ┣ 📜pack-0aff33162c0e8b17e796576c493a0b067ecb87f0.idx
 ┃ ┃ ┃ ┣ 📜pack-0aff33162c0e8b17e796576c493a0b067ecb87f0.pack
 ┃ ┃ ┃ ┗ 📜pack-0aff33162c0e8b17e796576c493a0b067ecb87f0.rev
 ┃ ┣ 📂refs
 ┃ ┃ ┣ 📂heads
 ┃ ┃ ┃ ┗ 📜main
 ┃ ┃ ┣ 📂remotes
 ┃ ┃ ┃ ┗ 📂origin
 ┃ ┃ ┃ ┃ ┗ 📜HEAD
 ┃ ┃ ┗ 📂tags
 ┃ ┣ 📜HEAD
 ┃ ┣ 📜config
 ┃ ┣ 📜description
 ┃ ┣ 📜index
 ┃ ┗ 📜packed-refs
 ┣ 📂app
 ┃ ┣ 📂src
 ┃ ┃ ┣ 📂__pycache__
 ┃ ┃ ┃ ┗ 📜spark_utils.cpython-38.pyc
 ┃ ┃ ┣ 📜q1.py
 ┃ ┃ ┣ 📜q2.py
 ┃ ┃ ┣ 📜q3.py
 ┃ ┃ ┣ 📜q4.py
 ┃ ┃ ┣ 📜q5.py
 ┃ ┃ ┗ 📜spark_utils.py
 ┃ ┣ 📜.gitignore
 ┃ ┣ 📜README.md
 ┃ ┣ 📜requirements.txt
 ┃ ┗ 📜run_all.sh
 ┣ 📂project_data
 ┃ ┣ 📂LA_Crime_Data
 ┃ ┃ ┣ 📜LA_Crime_Data_2010_2019.csv
 ┃ ┃ ┗ 📜LA_Crime_Data_2020_2025.csv
 ┃ ┣ 📜LA_Census_Blocks_2020.geojson
 ┃ ┣ 📜LA_Census_Blocks_2020_fields.csv
 ┃ ┣ 📜LA_Police_Stations.csv
 ┃ ┣ 📜LA_income_2021.csv
 ┃ ┣ 📜MO_codes.txt
 ┃ ┗ 📜RE_codes.csv
 ┣ 📜.gitignore
 ┣ 📜Dockerfile
 ┣ 📜README.md
 ┣ 📜docker-compose.yml
 ┗ 📜run_git.sh