from kafka import KafkaConsumer
import os


class WriteToTopic:

    def __init__(self):
        self.one = ''
        # self.server = os.environ.get('server')
        # self.topic = os.environ.get('topic')
        # self.group = os.environ.get('group')
        # self.dictKafkaIdent = {
        #     'TOPIC': self.topic,
        #     'SERVER': self.server,
        #     # PROD
        #     'GROUP': self.group,
        #     'OFFSET': 'earliest'}

    def read_json_from_topic(self):
        consumerClassified = KafkaConsumer('test',
                                           group_id='my-group',
                                           bootstrap_servers=['host.docker.internal:9092']
                                           )
        consumerClassified.poll()
        consumerClassified.seek_to_beginning()
        for index, message in enumerate(consumerClassified):
            print(message)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    w = WriteToTopic()
    w.read_json_from_topic()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
