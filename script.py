#Importing boto3 and datetime modules
from shutil import ExecError
import boto3
from datetime import datetime, timedelta
#Declare Buckets 
Main_Bucket = 'shreyankbucket'
Aux_Bucket = 'backupshreyank'
#Declare S3 resource
s3 = boto3.resource('s3')
#Declare S3 client 
s3client = boto3.client('s3')
#Declared a paginator: Paginator allows you to retrive all data from bucket in one line of code vs multiple requests (aws sends 1000 results at one time)
paginator = s3client.get_paginator('list_objects_v2')
#Keeps track of amount of pages in bucket
page_iterator = paginator.paginate(Bucket=Main_Bucket)
#Declare loop to iterate through iterator
#debug : storing data in list
objects=[]
for page in page_iterator:
    #To read bucket objects
    for object in page['Contents']:
        #Need to check files that are older than 6 months, 180 days
        #Accessing files based on LastModified 
        #try-except will handle crash if no files are found
        try:
            if object['LastModified'] < datetime.now().astimezone() - timedelta(seconds=30): 
                #for now in seconds,will change it later it to days 
                #Printing files for debug
                objects.append(object['Key'])
                print(objects)
                print(f"File Name:{object['Key']}")
                #Creating copy of matched files into backup bucket
                #Putting it in try block to handle errors
                try:
                    s3client.copy_object(
                        Bucket=Aux_Bucket,
                        Key=object['Key'],
                        CopySource={'Bucket':Main_Bucket, 'Key':object['Key']}
                    )
                except Exception as e:
                    #print error for debug purpose
                    print('Error occured while copying files between buckets',e)
                    #Delete will remove the matched files from main bucket
                try:
                    s3client.delete_object(Bucket=Main_Bucket, Key=object['Key'])
                except Exception as e:
                    #Print error for debug purpose
                    print('Error ocuured in deletion',e)
        except ExecError as e:
            print('No files found')
