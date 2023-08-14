import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from flask import Flask, request, jsonify
from flask_cors import CORS

# Replace with your Azure Blob Storage details
connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakeinnovate;AccountKey=2KrZsQC3H/IJk6QLNbC2haRiTyl59sdWeUJNwPEe/K5TRuZ+m3bw95LJ1d7hy7RaYr/kQ+cZXl1Z+ASt194giw==;EndpointSuffix=core.windows.net'
container_name = 'democontainer'
csv_file_path = 'New Report blob.csv'  # The path within the container, e.g., if the file is at the root level

app = Flask(__name__)
CORS(app)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_customer_data(customer_name):
    try:
        logging.info('Connecting to Azure Blob Storage...')
        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a BlobClient for the CSV file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_file_path)

        logging.info('Downloading the CSV file from Azure Blob Storage...')
        # Download the CSV file to a local file
        local_file_name = 'temp_file.csv'
        with open(local_file_name, "wb") as local_file:
            blob_client.download_blob().readinto(local_file)

        # Load the CSV file into a pandas DataFrame
        logging.info('Loading data from CSV into a DataFrame...')
        df = pd.read_csv(local_file_name)

        # Filter the DataFrame based on customer_name
        logging.info('Filtering data based on customer_name...')
        result = df[df['Surname'] == customer_name]

        return result.to_dict('records')

    except Exception as e:
        logging.error(f"Error: {e}")

        return []

@app.route('/', methods=['POST'])
def index():
    if request.method == 'POST':
        data = request.get_json()
        customer_name = data.get('customer_name')

        if customer_name:
            # Get customer data from Azure Blob
            customer_data = get_customer_data(customer_name)

            # Return the customer data as JSON response
            return jsonify(customer_data)

        return jsonify([])  # Return an empty list if customer_name is not provided

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5005, use_reloader=True)





# import logging
# import pandas as pd
# from azure.storage.blob import BlobServiceClient
# from flask import Flask, request, jsonify, send_file
# from flask_cors import CORS

# # Replace with your Azure Blob Storage details
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakeinnovate;AccountKey=2KrZsQC3H/IJk6QLNbC2haRiTyl59sdWeUJNwPEe/K5TRuZ+m3bw95LJ1d7hy7RaYr/kQ+cZXl1Z+ASt194giw==;EndpointSuffix=core.windows.net'
# container_name = 'democontainer'
# csv_file_path = 'New Report.csv'  # The path within the container, e.g., if the file is at the root level

# app = Flask(__name__)
# CORS(app)

# # Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_customer_data(customer_name):
#     try:
#         logging.info('Connecting to Azure Blob Storage...')
#         # Create a BlobServiceClient using the connection string
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#         # Get a BlobClient for the CSV file
#         blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_file_path)

#         logging.info('Downloading the CSV file from Azure Blob Storage...')
#         # Download the CSV file to a local file
#         local_file_name = 'temp_file.csv'
#         with open(local_file_name, "wb") as local_file:
#             blob_client.download_blob().readinto(local_file)

#         # Load the CSV file into a pandas DataFrame
#         logging.info('Loading data from CSV into a DataFrame...')
#         df = pd.read_csv(local_file_name)

#         # Filter the DataFrame based on customer_name
#         logging.info('Filtering data based on customer_name...')
#         result = df[df['Surname'] == customer_name]

#         return result.to_dict('records')

#     except Exception as e:
#         logging.error(f"Error: {e}")

#         return []

# @app.route('/', methods=['POST'])
# def index():
#     if request.method == 'POST':
#         data = request.get_json()
#         customer_name = data.get('customer_name')

#         if customer_name:
#             # Get customer data from Azure Blob
#             customer_data = get_customer_data(customer_name)

#             # Load the CSV file into a pandas DataFrame
#             df = pd.read_csv(csv_file_path)

#             # Filter the DataFrame based on customer_name
#             result = df[df['Surname'] == customer_name]

#             # Return the filtered data as JSON
#             return jsonify(result.to_dict('records'))
            

#             # Return the customer data as JSON response
#             return jsonify(customer_data)
        
#         return jsonify([])  # Return an empty list if customer_name is not provided
        
#     # Serve the temporary CSV file for download
#     return send_file(csv_file_path, as_attachment=True)

       

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5005, use_reloader=True)