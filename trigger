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
