import json
import requests
from os.path import expanduser
import boto3
import datetime
import cx_Oracle
import sys
import os
import filelogging
from datetime import date

#this program basically submits SNS messages to an SNS topic for daily data publications

def load_config_json(env:str, region: str):
"""
this method loads a json file and obtains bucket, keypath and filepath. Parameters being table name and use case and finally returns dictionary

    filelogging.log.info (" Attempting to load json file")
    filedir = os.path.dirname(os.path.realpath(_file_))
    file = os.path.join(filedir, "../config/Parameters.json")
    filelogging.log.info("File Path: " +file)
    try:
        filelogging.log.info("Loading config file")
        with open(file) as json_data_file:
            config = json.load(json_data_file)
            filelogging.log.info("Success")

    except: 
        filelogging.log.error("File not found")
        raise Exception ("File Parameters.json not found")

    try:  
        filelogging.log.info("local s3 bucket")
        local_s3_bucket = config["environment"][env]["local_s3_buckets"][region]
        filelogging.log.info("local_s3_bucket: " + local_s3_bucket)
    
    sns_topic = config["environment"][env]["sns_topics"][region]["trigger"]
    
    #similarly fetch db details
    
    #db_username = config["environment"][env]["db_info"]["user_name"]
    #db_password = ... ["password"]
    
    return {
      "local_s3_bucket": local_s3_bucket,
      "sns_topic": sns_topic
      }
      
      
except: 
    filelogging.log.error("couldn't fetch details")
    raise Exception ("No luck fetching details") 
    
    
    
    
    
def trigger(args):
    filelogging.log.info("Fetching region")
    aws_reg= get_reg()
    filelogging.log.info("Region got")
    
    trigger_final_json_format = {
      "workflowname": args[0],
      "env": args[1]
      "jobId" : args[2],
      "d_as_of_dt" : args[3]
      }
 
    filelogging.log.info("Sending SMS messages: " +str(trigger_final_json_format))
    send_sns_message(trigger_final_json_format, aws_reg, args[4])
    
    
    
def get_reg():

    response = requests.get('link')
    try:
        aws_reg= str(response.text[:-1])
        filelogging.log.info("Got!")
    
    except:
        raise Exception("AWS region not found")
    
    return aws_reg
    
    
    
    
def convert_date(odate):
    input_date = datetime.datetime.strptime(odate, '%Y%m%d')
    return input_date.strftime('%d-%b-%y').upper()
    
    

def fetch_env():
    filelogging.log.info("environment")
    acc = boto3.client("xyz").get_id()["acc"]
    if acc = '1':
      env="qa"
    elif acc ='2':
      env = "dev"
      
    return env
    
    
    
def send_msg(msg, aws_reg, sns_topic):
    sns = boto3.client(
          'sns',
          reg_name = aws_reg
    )
    
    
    with open(expanduser('~') + ' /path/' +'.json', 'a+' as f:
        f.write('\n' + json.dumps(msg))
        
        response = sns.publish(
                    topic=sns_topic,
                    msg = json.dumps(msg)
                    )
                    
                    
        if response.get('Mid'):
            print('Msg sent')
        else:
            print('Not send')
            
            
            
if __name__ == '__main__':
    try:
        if len(sys.argv) <3 :
            print("Not enough params")
            sys.exit(1)
    except: 
        filelogging.log.error("Couldn't accept run time params")
        raise Exception("Unacceptable run time params")
odate_input = sys.argv[1]
fin_dt =  datetime.datetime.strptime(str(date.today()),'%Y-%m-%d').strftime
now=datetime.datetime.now()
jobId = odate_input+fin_dt+str(now.hour).zfill(2)+str(now.minute).zfill(2)
odate = convert_date(odate_input)
local_dr= os.path.join(filedir, '../path/log')

if not os.path.exists(local_dr):
    os.makedirs(local_dr)
    
s3_prefix = "path/log/" + odate + "/"
file_nm = jobId + "_" +odate+"_"+".log"
usecase= sys.argv[2].lower()


try:
    filedir2 = os.path.dirname(os.path.realpath(__file__))
    filelogging.log.info("Reading stuff")
    file_wf=os.path.join(filedir2, "../path/file.json")
    
    with open(file_wf) as json_data:
        config= json.load(json_data)
        
        
except:
      filelogging.log.error("File not found")
      raise Exception ("file not found")
      
      
      
try:
    workflows = config["company"]["workflows"][usecase]
    if len(sys.argv) > 3: 
        if i.upper+"_BRIDGE" not in workflows:
            print(i.upper()+" is not valid" +usecase)
            sys.exit(1)
            
except:

    filelogging.log.error("Not a valid workflow name")
    raise Exception( "Not a valid workflow")
    
    
env = get_env()
if env=="dev":
    fin_env = "dev-poc"
    sec_env = "ofsaa_qa"
else:
    fin_env = "prod"
    sec_env = "ofsaa_prod"
    
region = get_reg()

if len(sys.argv) > 3:
    for i in sys.argv[3:]:
        wfname = i.upper() + "_BRIDGE"
        lst = [wfname, fin_env, jobId, odate, sns_topic]
        trigger(lst)
        
        
elif len(sys.argv)==3:
    for i in range(0, len(workflows)):
        lst = [workflows[i], fin_env, jobId, odate, sns_topic]
        
        
filelogging.local_log_create(local_log_dir, logfilename)
filelogging.s3_log_create(local_s3_bucket, log_file_s3_key)
        

    
    

    
