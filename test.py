import botocore
import boto3
import numpy as np
import cv2
from tqdm import tqdm

s3 = boto3.client('s3')

url = s3.generate_presigned_url('get_object', Params={"Bucket":'ccdb-cw-bucket', "Key":'videos/video.mp4'})

reader = cv2.VideoCapture(url)
count = 0
while True:
    
    for i in range(int(reader.get(cv2.CAP_PROP_FRAME_COUNT))):
        _, image = reader.read()

        # image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
        # assert(bytes(image) == image.tobytes())


        producer.send('frames', 
                      value=json.dumps({'index': i, 'frame': image}, cls=NumpyEncoder).encode('utf-8'), 
                      key='video.mp4'.encode('utf-8'))
    
    count += 1
    print("streamed vidoe %i times, restarting..." % count)

    # reader.release()
    # producer.flush()


reader.release()
