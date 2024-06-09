import pandas as pd
import re


def data_staging(df):

    """This is Data Staging Stage here the data is cleaned
        The dataframe, consists of filename and it's corresponding data
        We FIRST filter out the logs that has any "transaction" details and create it's dataframe
        Within each row there are still multiple "transaction" so we further  split it on bases on "START RequestId" & "END RequestId"
        And then the remaining using "Processing message"
    """
    # Filter the dataframe to only include rows where the 'data' column contains "transaction: {"
    filtered_df = df[df['data'].str.contains("transaction: {", regex=False)]

    # Initialize lists to store transaction blocks and their corresponding file names
    transactions = []
    file_names = []

    # Iterate over the rows of the filtered dataframe
    for _, row in filtered_df.iterrows():
        # Split the data into lines
        lines = row['data'].split('\n')

        # Initialize variables to store transaction block and flag
        transaction_block = []
        inside_transaction = False

        for line in lines:
            if re.search(r"START RequestId", line):
                inside_transaction = True
                transaction_block.append(line)
            elif re.search(r"END RequestId", line):
                transaction_block.append(line)
                inside_transaction = False
                transactions.append("\n".join(transaction_block))
                file_names.append(row['file_name'])
                transaction_block = []
            elif inside_transaction:
                transaction_block.append(line)

    # Create a new dataframe with each row containing one transaction
    transaction_df = pd.DataFrame({'file_name': file_names, 'transaction_data': transactions})

    # Initialize lists to store split messages and their corresponding file names
    split_transactions = []
    file_names = []

    # Iterate over the rows of the transaction dataframe
    for _, row in transaction_df.iterrows():
        # Split the transaction data by "Processing message"
        messages = row['transaction_data'].split('Processing message')

        # Iterate over each message and store them as separate rows
        for message in messages:
            if message.strip():  # Check if the message is not empty
                split_transactions.append('Processing message' + message)
                file_names.append(row['file_name'])

    # Create a new dataframe with each row containing one split message
    split_transaction_df = pd.DataFrame({'file_name': file_names, 'transaction_data': split_transactions})

    # Filter the dataframe to only include rows where the 'transaction_data' column contains "transaction"
    final_transaction_df = split_transaction_df[
        split_transaction_df['transaction_data'].str.contains("transaction", regex=False)]

    return final_transaction_df
