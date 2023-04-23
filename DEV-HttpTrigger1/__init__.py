import azure.functions as func 
from azure.storage.blob import BlobServiceClient 
    #pip install azure-storage-blob 
import pandas as pd 
    #pip install pandas 
import datetime 
import os

def main(req: func.HttpRequest) -> func.HttpResponse: 
    # INPUTSTORAGEACCOUNTURLSAS= "{SAS Key to Employee file}" 
    # STORAGEACCOUNTSAS="{SAS Key to Output Location}"
    # 14/03 up to 26/03/2023 13:42
    # Folder : INPUTSTORAGEACCOUNTURLSAS= "https://blobchainbetweensoobrdev.blob.core.windows.net/raw-data/people?sv=2020-02-10&st=2023-03-17T12%3A42%3A52Z&se=2023-03-26T11%3A42%3A00Z&sr=d&sp=rl&sig=3crp0vh2ssoOXxH6u9bBjjzYu4XTxVsOY%2BmA9GQ%2Bz4w%3D&sdd=1" 
    # File input
    INPUTSTORAGEACCOUNTURLSAS="https://blobchainbetweensoobrdev.blob.core.windows.net/raw-data/people/employee.csv?sv=2021-10-04&st=2023-03-17T14%3A41%3A36Z&se=2023-03-26T13%3A41%3A00Z&sr=b&sp=r&sig=aQ8peY6f%2BLzEKwaown2O%2Bv%2FD81POVTYqsXCOOQwXohQ%3D"
    # Output container
    STORAGEACCOUNTSAS="SharedAccessSignature=sv=2021-10-04&ss=btqf&srt=sco&st=2023-03-17T14%3A55%3A06Z&se=2023-03-26T13%3A55%3A00Z&sp=rwdxl&sig=in14bg9elFaYK316BEq0Rz3VeKpFwPXzrcOTsTN2Xx4%3D;BlobEndpoint=https://blobchainbetweensoobrdev.blob.core.windows.net/;FileEndpoint=https://blobchainbetweensoobrdev.file.core.windows.net/;QueueEndpoint=https://blobchainbetweensoobrdev.queue.core.windows.net/;TableEndpoint=https://blobchainbetweensoobrdev.table.core.windows.net/;"
    
    #Read CSV directly from Blob location
    df = pd.read_csv(INPUTSTORAGEACCOUNTURLSAS,delimiter = ',')
    print(df.info())
    # try:
    # except Exception as e: 
    #     print(e)
    #     logging.info("Unable to read file from BlobStorage.\n"+str(e))

    print('Python HTTP trigger function read input file(s).')

    #Do the data manipulation
    df['no_of_emp']= 1
    df['joining_year']= pd.DatetimeIndex(df['joining_date']).year
    df_result = df.groupby('joining_year').count()[['no_of_emp']]
    output = df_result.to_csv(encoding = "utf-8")


    #Connect to Output location of Azure Blob 
    blob_service_client = BlobServiceClient.from_connection_string(STORAGEACCOUNTSAS) 
    output_dir = "output/"
    output_file = "employeecountbyyear.csv"

    # Instantiate a new ContainerClient 
    container_client = blob_service_client.get_container_client("prod-data") 
    print('Python HTTP trigger function connected to output container and retrieved it.')
    # Instantiate a new BlobClient 
    # blob_client = container_client.get_blob_client("output/employeecountbyyear.csv") 
    blob_client = container_client.get_blob_client(output_dir+output_file) 
    if blob_client.exists(): 
        blob_client.delete_blob() 

    print('Python HTTP trigger function updated blob locally.')

    try:
        # container_client.upload_blob(name="output/employeecountbyyear.csv", data=output, overwrite=True) 
        container_client.upload_blob(name=output_dir+output_file, data=output, overwrite=True) 
    except Exception as e: 
        print(e)   
