import csv
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='CSV input file'
                            )
        parser.add_argument('--output',
                            help='JSON Output Location'
                            )


def print_row(element):
    print(element)


def parse_csv_file(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        line[0] = line[0].replace('{', '').replace('}', '')
        return line


def list_to_json_line(element):
    return json.dumps(element)


def main():
    input_file = '/Users/chris/Development/sample_data_pipeline_beam/pp-monthly-update-new-version.csv'
    output_file = ''

    with beam.Pipeline(options=options) as pipeline:
        parsed_csv = (
                pipeline
                | 'Read CSV file' >> beam.io.ReadFromText(input_file)
                | 'Convert file to PyObjects' >> beam.Map(parse_csv_file)
                | 'Print CSV output' >> beam.Map(print_row)
        )


if __name__ == '__main__':
    options = PipelineOptions()
    main()
