import unittest
import consumer_offset_reporter

new_consumer_response = [
    'GROUP, TOPIC, PARTITION, CURRENT OFFSET, LOG END OFFSET, LAG, OWNER\n',
    'some_group, some_topic, 36, 62275226, 62275309, 83, some_owner\n',
    'some_group, some_topic, 37, 60294854, 60294934, 80, some_owner\n',
    'some_group, some_topic, 14, 115501187, 115501292, 105, some_owner\n',
    'some_group, some_topic, 15, 112734196, 112734299, 103, some_owner\n',
]

old_consumer_response = [
    'GROUP, TOPIC, PARTITION, CURRENT OFFSET, LOG END OFFSET, LAG, OWNER\n',
    'some_group, some_topic, 0, 1015266052, 1015266344, 292, some_owner\n',
    'some_group, some_topic, 1, 1018553456, 1018553748, 292, some_owner\n',
    'some_group, some_topic, 2, 972496012, 972496300, 288, some_owner\n',
    'some_group, some_topic, 3, 145948526, 145948809, 283, some_owner\n',
]


class TestTranslator(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_parses_new_response(self):
        parsed = consumer_offset_reporter.parse_output(new_consumer_response)
        self.assertItemsEqual(parsed, [
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 36, 'current_offset': 62275226, 'log_end_offset': 62275309, 'lag': 83},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 37, 'current_offset': 60294854, 'log_end_offset': 60294934, 'lag': 80},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 14, 'current_offset': 115501187, 'log_end_offset': 115501292, 'lag': 105},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 15, 'current_offset': 112734196, 'log_end_offset': 112734299, 'lag': 103},
        ])

    def test_parses_old_response(self):
        parsed = consumer_offset_reporter.parse_output(old_consumer_response)
        self.assertItemsEqual(parsed, [
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 0, 'current_offset': 1015266052, 'log_end_offset': 1015266344, 'lag': 292},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 1, 'current_offset': 1018553456, 'log_end_offset': 1018553748, 'lag': 292},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 2, 'current_offset': 972496012, 'log_end_offset': 972496300, 'lag': 288},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 3, 'current_offset': 145948526, 'log_end_offset': 145948809, 'lag': 283},
        ])

    def test_to_line_protocol(self):
        parsed = consumer_offset_reporter.to_line_protocol([
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 0, 'current_offset': 1015266052, 'log_end_offset': 1015266344, 'lag': 292},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 1, 'current_offset': 1018553456, 'log_end_offset': 1018553748, 'lag': 292},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 2, 'current_offset': 972496012, 'log_end_offset': 972496300, 'lag': 288},
            {'group': 'some_group', 'topic': 'some_topic', 'partition': 3, 'current_offset': 145948526, 'log_end_offset': 145948809, 'lag': 283},
        ])

        self.assertItemsEqual(parsed, [
            'kafka.consumer_offset,topic=some_topic,group=some_group,partition=0 current_offset=1015266052,log_end_offset=1015266344,lag=292',
            'kafka.consumer_offset,topic=some_topic,group=some_group,partition=1 current_offset=1018553456,log_end_offset=1018553748,lag=292',
            'kafka.consumer_offset,topic=some_topic,group=some_group,partition=2 current_offset=972496012,log_end_offset=972496300,lag=288',
            'kafka.consumer_offset,topic=some_topic,group=some_group,partition=3 current_offset=145948526,log_end_offset=145948809,lag=283'
        ])

if __name__ == "__main__":
    unittest.main()
