import csv
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    # Add some custom arguments to the Beam options so we can give input and output lcoations on the command line
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
    # read the file and de-quote the data
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # Handle replacing the extra curly brackets on the UUID
        line[0] = line[0].replace('{', '').replace('}', '')

        # Change the sale price to a number
        line[1] = int(line[1])

        # No point in converting the datetime in line[2] to a datetime object as JSON would dump it as string anyway

        return line


def list_to_json_line(element):
    return json.dumps(element)


def main():
    input_file = '/Users/chris/Development/sample_data_pipeline_beam/pp-monthly-update-new-version.csv'
    output_file = '/Users/chris/Development/sample_data_pipeline_beam/pp-monthly-update-new-version'

    # Use context handler so our pipeline can be closed automatically
    with beam.Pipeline(options=options) as pipeline:
        # Initally just load and parse the CSV into Beam Pipeline
        parsed_csv = (
                pipeline
                | 'Read CSV file' >> beam.io.ReadFromText(input_file)
                | 'Convert file to PyObjects' >> beam.Map(parse_csv_file)
                | 'Dump to List of JSON Lines' >> beam.Map(list_to_json_line)
                | 'Write the final transformed data to file' >> beam.io.WriteToText(output_file)
        )


if __name__ == '__main__':
    options = PipelineOptions()
    main()
