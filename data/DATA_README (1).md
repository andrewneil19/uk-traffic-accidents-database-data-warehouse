# Data Source

## Dataset

This project uses the **UK Road Safety: Traffic Accidents and Vehicles** dataset from Kaggle.

**Source:** https://www.kaggle.com/datasets/tsiaras/uk-road-safety-accidents-and-vehicles

## Setup Instructions

1. **Download the dataset** from the Kaggle link above
2. **Extract the zip file** - you'll get two large CSV files:
   - `Accident_Information.csv`
   - `Vehicle_Information.csv`

3. **Create a project data folder:**
   ```
   ProjectDataFiles/
   ├── Accident_Information.csv
   ├── Vehicle_Information.csv
   └── csvExtract.py
   ```

4. **Run the extraction script:**
   ```bash
   pip install codecs
   python csvExtract.py
   ```

5. **Output files** - The script creates random subsets:
   - `Accidents_extract.csv`
   - `Vehicles_extract.csv`

These extracted CSV files serve as the data sources for the ETL pipelines.

## Note on Repository

Due to file size constraints, the raw data files are not included in this repository. Follow the setup instructions above to obtain and prepare the data locally.
