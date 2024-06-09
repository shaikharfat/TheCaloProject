import pandas as pd
import re


def get_rich_df(df):
    """
    Creating the RICH data getting meaning data based on log
    Column details:
        date_time: Taken from the top of log message (Log Start Time)
        id: From transaction dictionary within log, It's the log id
        type: From transaction dictionary within log, values involve- CREDIT, DEBIT
        source: From transaction dictionary within log, values involve- MANUAL_ADDITION, PAYMENT etc
        action: From transaction dictionary within log, values involve- CUSTOMERS_CALO_EXPERIMENTS, RENEW_SUBSCRIPTION etc
        userId: From transaction dictionary within log, Its the user_id
        in_sync: Shows whether the balance is in_sync, to handle overdraft and early intervention in preventing overdraft

        other columns from transaction dict: paymentBalance, updatePaymentBalance, currency, amount, vat, oldBalance, newBalance
    """

    # Define a function to extract date_time, in_sync status, and split transaction data
    def process_transaction(transaction):
        # Extract date_time
        date_time_match = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', transaction)
        date_time = date_time_match.group(0) if date_time_match else None

        # Determine in_sync status
        in_sync = 'ERROR' not in transaction

        # Split transaction data
        transaction_values = re.split(r'[;,\s]\s*', transaction)

        return date_time, in_sync, transaction_values

    # Apply the function to the transaction_data column
    processed_data = df['transaction_data'].apply(process_transaction)

    # Create a new dataframe from the processed data
    new_df_updated = pd.DataFrame(processed_data.tolist(), columns=['date_time', 'in_sync', 'transaction_values'])

    # Define a function to extract the specific fields from the transaction values
    def extract_transaction_details(transaction_values):
        # Join the transaction values back into a string for easier extraction
        transaction_str = " ".join(transaction_values)

        # Extract relevant fields
        fields = {
            'id': None,
            'type': None,
            'source': None,
            'action': None,
            'userId': None,
            'paymentBalance': None,
            'updatePaymentBalance': None,
            'currency': None,
            'amount': None,
            'vat': None,
            'oldBalance': None,
            'newBalance': None
        }

        # Use regex to extract each field
        id_match = re.search(r'id:\s*\'([^\']*)\'', transaction_str)
        fields['id'] = id_match.group(1) if id_match else None

        type_match = re.search(r'type:\s*\'([^\']*)\'', transaction_str)
        fields['type'] = type_match.group(1) if type_match else None

        source_match = re.search(r'source:\s*\'([^\']*)\'', transaction_str)
        fields['source'] = source_match.group(1) if source_match else None

        action_match = re.search(r'action:\s*\'([^\']*)\'', transaction_str)
        fields['action'] = action_match.group(1) if action_match else None

        userId_match = re.search(r'userId:\s*\'([^\']*)\'', transaction_str)
        fields['userId'] = userId_match.group(1) if userId_match else None

        paymentBalance_match = re.search(r'paymentBalance:\s*(-?\d+\.?\d*)', transaction_str)
        fields['paymentBalance'] = float(paymentBalance_match.group(1)) if paymentBalance_match else None

        updatePaymentBalance_match = re.search(r'updatePaymentBalance:\s*(-?\w+)', transaction_str)
        fields['updatePaymentBalance'] = updatePaymentBalance_match.group(1) if updatePaymentBalance_match else None

        currency_match = re.search(r'currency:\s*\'([^\']*)\'', transaction_str)
        fields['currency'] = currency_match.group(1) if currency_match else None

        amount_match = re.search(r'amount:\s*(-?\d+\.?\d*)', transaction_str)
        fields['amount'] = float(amount_match.group(1)) if amount_match else None

        vat_match = re.search(r'vat:\s*(-?\d+\.?\d*)', transaction_str)
        fields['vat'] = float(vat_match.group(1)) if vat_match else None

        oldBalance_match = re.search(r'oldBalance:\s*(-?\d+\.?\d*)', transaction_str)
        fields['oldBalance'] = float(oldBalance_match.group(1)) if oldBalance_match else None

        newBalance_match = re.search(r'newBalance:\s*(-?\d+\.?\d*)', transaction_str)
        fields['newBalance'] = float(newBalance_match.group(1)) if newBalance_match else None

        return fields

    # Apply the function to extract transaction details and create a new dataframe
    transaction_details = new_df_updated['transaction_values'].apply(extract_transaction_details)
    details_df = pd.DataFrame(transaction_details.tolist())

    # Combine with the original columns for date_time and in_sync
    final_df = pd.concat([new_df_updated[['date_time', 'in_sync']], details_df], axis=1)
    final_df['date_time'] = pd.to_datetime(final_df['date_time'])

    return final_df
