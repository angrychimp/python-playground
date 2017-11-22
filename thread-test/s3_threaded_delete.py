import boto3
import sys
import json
import logging
from threading import Thread
from queue import Queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeleteWorker(Thread):
	def __init__(self, **args):
		Thread.__init__(self)
		self.queue = args['Queue']
		self.client = args['Client']

	def run(self):
		while True:
			# grab a task from the queue
			bucket, key = self.queue.get()
			try:
				logger.info('Removing object: %s/%s' % (bucket, key))
				self.client.delete_object(Bucket=bucket, Key=key)
			finally:
				self.queue.task_done()

def handler(event, context):
	client = boto3.client('s3')
	response = client.list_objects_v2( Bucket=event['Bucket'], Prefix=event['Prefix'] )
	queue = Queue()
	for x in range(8):
		worker = DeleteWorker(Queue=queue, Client=client)
		worker.daemon = True
		worker.start()
	for obj in response['Contents']:
		logger.info('Queuing %s/%s' % (event['Bucket'], obj['Key']))
		queue.put((event['Bucket'], obj['Key']))
	queue.join()
	logger.info('Bulk delete complete')


if __name__ == '__main__':
	event = json.loads(sys.argv[1])
	print(handler(event,{}))