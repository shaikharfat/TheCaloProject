import zipfile
import os
import gzip
import pandas as pd

from data_staging import *
from rich import get_rich_df


def main():
    zip_file_path = 'raw_data.zip'
    extracted_folder_path = 'unzipped_data'

    # Extracting the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder_path)

    # Reading .gz file
    def read_gz_file(file_path):
        with gzip.open(file_path, 'rt') as f:
            return f.read()

    # List the extracted files and directories
    extracted_files = []
    for root, dirs, files in os.walk(extracted_folder_path):
        for file in files:
            if file.endswith('.gz'):
                extracted_files.append(os.path.join(root, file))

    # Create a list of dictionaries to build the dataframe
    data = []
    for file in extracted_files:
        file_data = read_gz_file(file)
        data.append({'file_name': file, 'data': file_data})

    # Create the dataframe
    df = pd.DataFrame(data)

    # Send data to staging layer
    staged_df = data_staging(df)

    # Send staged_data to ge rich DF
    rich_df = get_rich_df(staged_df)

    """
    Overdraft Predictor Pro (OPP)
    
    This system detects accounts that are out of syn and will potentially go into overdraft
    It is based on error message recieved within the logs, which sets the in_sync flag to false and it can be observed
    that there is direct correlation when between in_sync and overdraft, which can proved by the following dataframe
    1) In overdraft_df it can be observed that in_sync flag is False for over 99% of negative "paymentBalance" (overdraft)
    2) In the random (overdraft)user_id lists it can be observed that in_sync predicts overdraft much before it happens and sets itself False 
    """

    grouped_sorted_df = rich_df.sort_values(by=["userId", "date_time"])
    overdraft_df = grouped_sorted_df[grouped_sorted_df["paymentBalance"]<0]

    user_ids = [
        "72b8eeff-c0f0-4665-bbbe-a1b1a6730a51",
        "00b2df99-692a-45c1-b277-854a63b3a168",
        "07cb8a00-acd7-4e6c-b4b1-ba09fecd14a4",
        "fa868335-ad20-473c-8442-c67a9ef35b23",
        "8c1ac27b-4b34-4d12-b590-9279bbe32c5d",
        "c7de1ac1-05c8-410e-a7d7-47fb25d705ba"
    ]

    # Filter the DataFrame to only include rows with the specified user IDs
    OPP_df = grouped_sorted_df[grouped_sorted_df['userId'].isin(user_ids)]

    """Uncomment to print the output of all step"""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    print("RAW DataFrame\n")
    print(df.head(5))
    print("\n" + "=" * 80 + "\n")

    print("Staged DataFrame\n")
    print(staged_df.head(5))
    print("\n" + "=" * 80 + "\n")

    print("RICH DataFrame\n")
    print(rich_df.head(50))
    print("\n" + "=" * 80 + "\n")

    print("Correlation between overdraft and in_sync\n")
    print(overdraft_df.head(1000))
    print("\n" + "=" * 80 + "\n")

    print("Overdraft Predictor Pro (OPP)\n")
    print(OPP_df.head(500))

    """Uncomment to write the output"""
    # df.to_csv("raw_df.csv")
    # staged_df.to_csv("staged_df.csv")
    # rich_df.to_csv("rich_df.csv")
    # overdraft_df.to_csv("overdraft.csv")
    # OPP_df.to_csv("overdraft_predictor_pro.csv")


if __name__ == "__main__":
    main()
