import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions


# pipeline
# 1. read text from an input data file
# 2. lines -> words
# 3. count word instances
# 4. sum the the count of each word
# 5. order by count descending
# 6. format each pair into a printable string
# 7. output to a file


class ExtractWordsDoFn(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element, **kwargs):
        line = element.strip().lower()
        words = re.findall(r'[\w\']+', line, re.UNICODE)
        return words


class FormatResult(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element, **kwargs):
        word, count = element
        yield '{}: {}'.format(word, count)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    return parser.parse_args()


def pipeline_options():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'my-project-id'
    google_cloud_options.job_name = 'myjob'
    google_cloud_options.staging_location = 'gs://your-bucket-name-here/staging'
    google_cloud_options.temp_location = 'gs://your-bucket-name-here/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'


def run():
    args = parse_args()
    logging.info('starting wordcount for {}'.format(args.input))

    options = PipelineOptions()

    # create the pipeline
    p = beam.Pipeline(options=options)

    # read the text file
    lines = p | 'read' >> beam.io.ReadFromText(args.input)

    # count the words
    counts = (lines
              | 'split' >> beam.ParDo(ExtractWordsDoFn())
              | 'count' >> beam.Map(lambda x: (x, 1))
              | 'group counts' >> beam.GroupByKey()
              | 'sum counts' >> beam.Map(lambda x: (x[0], sum(x[1])))
              )

    sorted_counts = counts

    # format the output
    formatted_output = sorted_counts | beam.ParDo(FormatResult()) # beam.Map(lambda x: '{}: {}'.format(x[0], x[1]))

    # write the output
    formatted_output | 'write' >> beam.io.WriteToText(args.output)
    p.run().wait_until_finish()


if __name__ == '__main__':
    run()
