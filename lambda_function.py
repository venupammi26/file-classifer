import json
import boto3


# boto3 S3 initialization
s3_client = boto3.client("s3")


def lambda_handler(event, context):
   destination_bucket_name = 'textract-input-pdf-images'

   # event contains all information about uploaded object
   print("Event :", event)

   # Bucket Name where file was uploaded
   source_bucket_name = event['Records'][0]['s3']['bucket']['name']
   print(source_bucket_name)

   # Filename of object (with path)
   file_key_name = event['Records'][0]['s3']['object']['key']
   print(file_key_name)
   file_key_name = file_key_name.replace("+"," ")
   # Copy Source Object
   copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
   print(copy_source_object)
   file_type(copy_source_object)
   # S3 copy object operation
   #s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key="pdf_file/"+file_key_name)

  
#Definition for getting filetype

def file_type(copy_source_object):
   file_name=copy_source_object["Key"].lower()
   
   if file_name.endswith(".pdf"):
      #send_to_folder(copy_source_object,"pdf_type/")
      get_pdf_type(copy_source_object)
   
   else:
      if file_name.endswith(".jpeg") or file_name.endswith(".jpg") or file_name.endswith(".png") :
         #send_to_folder(copy_source_object,"images_type/")
         copy_image(copy_source_object)
         
      else:
         if file_name.endswith(".csv") or file_name.endswith(".xlsx") or file_name.endswith(".xls"):
            send_to_folder(copy_source_object,"csv_type/")
         else:
            if file_name.endswith(".txt"):
               send_to_folder(copy_source_object,"text_type/")
            else:
               send_to_folder(copy_source_object,"other_type/")
      
      
#Definition for files copying to corresponding folders

def send_to_folder(copy_source_object,file_ext):
   destination_bucket_name = 'textract-input-pdf-images'
   file_key_name = copy_source_object["Key"]
   s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=file_ext + file_key_name)
   print(file_key_name+"  file copied successfully  into "+ file_ext)
   
 
#Definiton for moving image to scanned pdf bucket to extract text

def copy_image(copy_source_object):
    scanned_destination_bucket_name = "scanned-pdf-files-bucket"
    bucket_name =copy_source_object["Bucket"]
    item_name = copy_source_object["Key"]
    s3_client.copy_object(CopySource=copy_source_object, Bucket=scanned_destination_bucket_name, Key="images_type/" + item_name)
    print("image file successfully copied to the scanned-pdf-files-bucket")
    

 
#Definiton for checking scanned pdf or searchable pdf

from PyPDF2 import PdfFileReader
from io import BytesIO

def get_pdf_type(copy_source_object):
    print("get_pdf_type invoked")
    searchable_destination_bucket_name = "searchable-pdf-files-bucket"
    scanned_destination_bucket_name = "scanned-pdf-files-bucket"
    bucket_name =copy_source_object["Bucket"]
    item_name = copy_source_object["Key"]
    s3_client = boto3.client("s3")
    
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, item_name)
    
    text=""
    fs = obj.get()['Body'].read()
    pdfFile = PdfFileReader(BytesIO(fs))
    pageObj = pdfFile.getPage(0)
    text = pageObj.extractText()
   

    if text == "":
        s3_client.copy_object(CopySource=copy_source_object, Bucket=scanned_destination_bucket_name, Key="Scanned_Pdf/" + item_name)
        print("scanned pdf file successfully copied to the scanned-pdf-files-bucket")
    else:
        s3_client.copy_object(CopySource=copy_source_object, Bucket=searchable_destination_bucket_name, Key=item_name)
        print("Searchable Pdf successfully copied to the searchable-pdf-files-bucket") 
        
    

