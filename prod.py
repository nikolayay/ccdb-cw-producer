from kafka import KafkaProducer, KafkaAdminClient
from tqdm import tqdm
import cv2
from PIL import Image
import json
import numpy as np
import time
import botocore
import boto3
import sys

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

#admin = KafkaAdminClient(bootstrap_servers='172.31.76.215')

# get the video stream
s3 = boto3.client('s3')
url = s3.generate_presigned_url('get_object', Params={"Bucket":'ccdb-cw-bucket', "Key":'videos/video.mp4'})
if url == None:
    print("Error: renew s3 creds")
    sys.exit()


count = 0

# single (linear?) producer
producer = KafkaProducer(bootstrap_servers='172.31.76.215')

while True:
    try:

        # ! this is not freed, might be problematic 
        reader = cv2.VideoCapture(url)

        if not reader.isOpened():
            raise NameError('S3 creds probably outdated')

        if not producer.bootstrap_connected():
            raise NameError('producer cannot connect to cluster')

        for i in tqdm(range(int(reader.get(cv2.CAP_PROP_FRAME_COUNT)))):
            
            _, image = reader.read()
            
            # TODO INCREASE MESSAGE SIZE FOR HI-RES
            image = cv2.resize(image, (100, 100))

            # preprocessing
            # image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
            # assert(bytes(image) == image.tobytes())

            # send to workers
            producer.send('slags-3', 
                        value=json.dumps({'index': i, 'frame': image}, cls=NumpyEncoder).encode('utf-8'), 
                        key='video.mp4'.encode('utf-8'))

    except NameError as e:
        print(e)
        sys.exit(1)

    # reset count to stream continiously
    reader.set(cv2.CAP_PROP_POS_FRAMES, 0)
    count += 1

    print("streamed vidoe %i times, restarting..." % count)

