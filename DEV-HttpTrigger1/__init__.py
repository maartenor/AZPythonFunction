import os
import azure.functions as func 
from azure.storage.blob import BlobServiceClient 
import datetime as dt
from datetime import  timedelta

def parse_textfile(txt):
    """
    This function parses the input text and replaces escape characters with their corresponding symbols.
    
    Args:
    - txt: A string representing the text to be parsed.
    
    Returns:
    - A string representing the parsed text with escape characters replaced.
    """
    return str(txt).replace('\\"','"').replace('"[','[').replace(']"',']').replace('\\"','"')

def select_modified_blobs(container_client, filetype = '.json', dt_start = dt.datetime.now() + timedelta(days=-1), dt_end = dt.datetime.now()):
    """
    Selects blobs from a container that were last modified between two datetime objects and match a specified file type.

    Args:
    - container_client: An instance of BlobServiceClient.get_container_client().
    - filetype (optional): A string specifying the file type to match. Default is '.json'.
    - dt_start (optional): A datetime object specifying the start time for selecting modified blobs. Default is one day before the current time.
    - dt_end (optional): A datetime object specifying the end time for selecting modified blobs. Default is the current time.

    Returns:
    - A list of blob names that were selected based on the specified criteria.
    """

    blobs_list = list(container_client.list_blobs())
    blobs_list
    blobs_selected = []
  
    for val in blobs_list:
        if (val['last_modified'].timestamp() >= dt_start.timestamp()) \
            & (val['last_modified'].timestamp() < dt_end.timestamp()) :
            if str(val['name']).find(filetype) >= 0: 
                print(val['name'], '\t', val['last_modified'])
                blobs_selected += [val['name']]
    
    return blobs_selected
    
def download_blob_to_file(self, blob_service_client: BlobServiceClient, container_name):
    """
    Downloads a blob from the specified container in the given BlobServiceClient to a local file.

    Args:
        blob_service_client (BlobServiceClient): The BlobServiceClient instance used to access the blob storage.
        container_name (str): The name of the container that holds the blob to download.

    Returns:
        str: The downloaded blob content after performing a text file parsing operation.

    Raises:
        ValueError: If the blob name is not defined.
        FileNotFoundError: If the specified folder path does not exist.

    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=self.name)
    folderpath = '/'.join(str(self['name']).split('/')[:-1])
    filename = str(self['name']).split('/')[-1]
    pathfile = os.path.join(folderpath, filename)
    if not os.path.exists(pathfile):
        if not os.path.exists(folderpath):
            os.mkdir(folderpath)
        mode = 'xb'
    else: mode = 'wb'

    download_stream = blob_client.download_blob().readall()
    parsed = parse_textfile(download_stream.decode('utf8'))

    with open(file=pathfile, mode=mode) as sample_blob: 
        # sample_blob.write(download_stream.readall())
        sample_blob.write(parsed.encode())
    
    print('Written blob content to local file : ', pathfile)

    return parsed

def upload_blob_file(self, blob_service_client: BlobServiceClient, container_name):
    """
    Uploads a local file to an Azure Blob Storage container using the given
    BlobServiceClient instance and container name.

    Parameters:
    - blob_service_client: A BlobServiceClient instance that provides access
        to the Azure Blob Storage account.
    - container_name: A string that specifies the name of the container to
        upload the file to.

    Returns:
    None.

    Raises:
    - AzureError: If the upload fails or there is an error with the BlobServiceClient.
    - FileNotFoundError: If the local file specified by the path in the 'name'
        field of the class instance does not exist.
    """
    container_client = blob_service_client.get_container_client(container=container_name)
    folderpath = '/'.join(str(self['name']).split('/')[:-1])
    filename = str(self['name']).split('/')[-1]
    pathfile = os.path.join(folderpath, filename)

    with open(file=pathfile, mode="rb") as data:
        blob_client = container_client.upload_blob(name=pathfile, data=data, overwrite=True)
    
    print('Uploaded : ', pathfile)

def log(txt, filename='logs/log.log'):
    """
    Appends the input text to a log file at the specified file path.
    
    Args:
    - txt (str): The text to be logged.
    - filename (str): The file path of the log file to which the text should be appended. 
                      Default value is 'logs/log.log'.
    
    Returns:
    - filename (str): The file path of the log file.
    """
    if not os.path.exists(filename):
        folderpath = '/'.join(filename.split('/')[:-1])
        if not os.path.exists(folderpath):
            os.mkdir(folderpath)

    try:
        try:
            with open(filename, 'a') as f:
                f.writelines(str(txt)+'\n')
        except:
            with open(filename, 'x') as f:
                f.writelines(str(txt)+'\n')            
        f.close()
    except Exception as e: 
        print('Unable to write to log file', e)
    
    def return_filename(filename):
        print(filename)
        return filename
    


def main(req: func.HttpRequest) -> func.HttpResponse: 
    """
    Azure Function that connects to a specific blob container and downloads and uploads blobs
    
    Args:
    - req (func.HttpRequest): HTTP request object
    
    Returns:
    - func.HttpResponse: HTTP response object
    
    """
    
    container_name = "feedback-now-raw"

    # INPUTSTORAGEACCOUNTURLSAS= "{SAS Key to Employee file}" 
    # STORAGEACCOUNTSAS="{SAS Key to Output Location}"
    # STORAGEACCOUNTSAS=STORAGEACCOUNTSAS_connectionstring
    # 204/30/2023 13:42 up to 01-05
    # File input
    INPUTSTORAGEACCOUNTURLSAS="https://blobchainbetweensoobrdev.blob.core.windows.net/raw-data/people/employee.csv?sv=2021-10-04&st=2023-03-17T14%3A41%3A36Z&se=2023-03-26T13%3A41%3A00Z&sr=b&sp=r&sig=aQ8peY6f%2BLzEKwaown2O%2Bv%2FD81POVTYqsXCOOQwXohQ%3D"
    # Top level container
    STORAGEACCOUNTSAS_connectionstring="BlobEndpoint=https://blobhasfeedbacknowdev.blob.core.windows.net/;QueueEndpoint=https://blobhasfeedbacknowdev.queue.core.windows.net/;FileEndpoint=https://blobhasfeedbacknowdev.file.core.windows.net/;TableEndpoint=https://blobhasfeedbacknowdev.table.core.windows.net/;SharedAccessSignature=sv=2021-12-02&ss=bf&srt=sco&sp=rwdlacyx&se=2023-05-05T16:33:36Z&st=2023-04-30T08:33:36Z&spr=https&sig=zl5zl5DLI%2FdmXfEBD3Tw1Q%2FYg3iqstJJcCFbMAaeJpU%3D"

    #Connect to file location of Azure Blob 
    blob_service_client = BlobServiceClient.from_connection_string(STORAGEACCOUNTSAS_connectionstring) 
    container_client = blob_service_client.get_container_client("feedback-now-raw") 

    
    try:
        print(blobs_list[1]['name'])
    except: pass

    blobs_list = list(container_client.list_blobs())

    blob_list = select_modified_blobs(container_client)
    
    for blobby in blob_list:
        try:
            download_blob_to_file(blobby, 
                                  blob_service_client=blob_service_client, 
                                  container_name=container_name)
        except Exception as e: 
            try:
                log(e)
            except:
                print(e) 
        # parsing txt content to correct json syntax performed during download
        try:
            upload_blob_file(blobby, 
                             blob_service_client=blob_service_client, 
                             container_name=container_name)
        except Exception as e: 
            try:
                log(e)
            except:
                print(e) 