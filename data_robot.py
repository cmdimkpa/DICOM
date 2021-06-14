# Data Robot Module

import os
from os.path import join, isdir, isfile, dirname, abspath
from os import listdir
from time import sleep
import subprocess
from random import random
import datetime
import json
from pydicom import dcmread
import matplotlib.image as Image

local_dir = dirname(abspath(__file__)) if '__file__' in globals() else os.getcwd()
S3_INPUT_BUCKET = 'dicom-streaming-store-raw'
S3_OUTPUT_BUCKET = 'dicom-streaming-store-output'
AWS_PROFILE = 'dicom-streaming'
with open(join(local_dir, 'attributes.json'), 'r') as f:
    attributes = json.load(f) # this should be in the target directory

def now():
    return datetime.datetime.today()

def elapsed_secs(t):
    return (now() - t).total_seconds()

def get_filelist(path):
    return [join(local_dir, f) for f in listdir(path) if isfile(f)] if isdir(path) else []

def randkey(filepath, n_randtext=6):
    stub = filepath.split('/')[-1]
    for i in [' ', '-', '.']:
        stub = stub.replace(i, '_')
    randtext = str(random()).split('.')[-1][:n_randtext]
    parts = stub.split('_')
    rest = parts[:-1]
    ext = parts[-1]
    return '%s_%s.%s' % ('_'.join(rest), randtext, ext)

def isgoodfilepath(path):
    return '_' in path and ' ' not in path and len(path.split('_')[-1].split('.')[0]) == 6 

def copy_to_s3(filepath, s3bucket, aws_profile, writekey=None):
    key = writekey if writekey else filepath.split('/')[-1] if isgoodfilepath(filepath) else randkey(filepath)
    cmd = "aws s3 cp X s3://%s/%s --profile=%s%s" % (s3bucket, key, aws_profile, ' --content-type application/dicom' if key[-4:].lower() == '.dcm' else '')
    instrs = cmd.split(' ')
    instrs[3] = filepath
    subprocess.Popen(instrs) # async write

def extract_and_write_metadata(__file):
    try:
        ds = dcmread(__file, force=True)
        # read and save image data
        image_png_key = randkey('dicom-image.png')
        image_pdf_key = randkey('dicom-image.pdf')
        for key in [image_png_key, image_pdf_key]:
            item = join(local_dir, key)
            Image.imsave(item, ds.pixel_array)
        # write metadata item
        raw_dict = json.loads(ds.to_json())
        conv_dict = {attributes[k]:v['Value'][0] for k,v in raw_dict.items() if k in attributes and 'Value' in v}
        conv_dict['image_png'] = 'https://dicom-streaming-store-raw.s3.amazonaws.com/%s' % image_png_key
        conv_dict['image_pdf'] = 'https://dicom-streaming-store-raw.s3.amazonaws.com/%s' % image_pdf_key
        patient_id = conv_dict['Patient ID'] if 'Patient ID' in conv_dict else 'unknown'
        creation_date = conv_dict['Instance Creation Date'] if 'Instance Creation Date' in conv_dict else 'unknown'
        fname = 'TABLE_DATA_%s_%s.json' % (patient_id, creation_date)
        with open(join(local_dir, fname), 'w') as f:
            f.write(json.dumps(conv_dict))
    except Exception as err:
        print(str(err))

def process_files(files):
    def process_file(__file):
        copy_to_s3(__file, S3_OUTPUT_BUCKET if 'TABLE_DATA' in __file else S3_INPUT_BUCKET, AWS_PROFILE)
        extract_and_write_metadata(__file)
    return [process_file(f) for f in files]

class Listener:
    def __init__(self, active_path=local_dir, scan_interval=3, clean_interval=60*5):
        self.active_path = active_path
        self.scan_interval = scan_interval
        self.clean_interval = clean_interval
        self.path_register = get_filelist(self.active_path)
        self.to_clean = []
    def new_active_path(self, path):
        self.active_path = path
    def new_scan_interval(self, interval):
        self.scan_interval = interval
    def cleanup(self):
        ready = [filepath for filepath,logged in self.to_clean if elapsed_secs(logged) > self.clean_interval]
        for __file in ready:
            try:
                os.remove(__file)
            except:
                pass
    def scan(self):
        while True:
            current = get_filelist(self.active_path)
            new = [i for i in current if i not in self.path_register]
            if new:
                self.path_register = current
                self.to_clean.extend([(i, now()) for i in new])
                process_files(new)
            self.cleanup()
            sleep(self.scan_interval)
    
Listener().scan()