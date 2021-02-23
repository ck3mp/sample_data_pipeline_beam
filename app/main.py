import csv
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    # Add some custom arguments to the Beam options so we can give input and output lcoations on the command line
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='CSV input file',
                            default='/Users/chris/Development/sample_data_pipeline_beam/pp-monthly-update-new-version.csv'
                            )
        parser.add_argument('--output',
                            help='JSON Output Location',
                            default='/Users/chris/Development/sample_data_pipeline_beam/output/pp-monthly-update-new-version'
                            )


def parse_csv_file(element):
    # read the file and de-quote the data
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        # Handle replacing the extra curly brackets on the UUID
        line[0] = line[0].replace('{', '').replace('}', '')

        # Change the sale price to a number
        line[1] = int(line[1])

        # No point in converting the datetime in line[2] to a datetime object as JSON would dump it as string anyway

        return line


class JSONDump(beam.DoFn):
    def process(self, element):
        return [json.dumps(element)]


def create_output():
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    s3_prefix = f'PROCESSED/extracted_year={year}/extracted_month={month}/extracted_day={day}/{now.isoformat()}_output'
    return s3_prefix


def main():
    # Use S3 Data Lake Structuring for Athena/Glue for future cataloging etc.
    out_key = create_output()

    # Use context handler so our pipeline can be closed automatically and means execution is run automatically
    with beam.Pipeline(options=CustomOptions()) as p:
        # Load the CVS, Parse it into Python Objects, Turn to JSON Strings, and Write to File
        _ = (
                p
                | 'Read CSV file' >> beam.io.ReadFromText(p.options.input)
                | 'Convert file to PyObjects' >> beam.Map(parse_csv_file)
                | 'Dump to List of JSON Lines' >> beam.ParDo(JSONDump())
                | 'Write the final transformed data to file' >> beam.io.WriteToText(out_key)
        )


if __name__ == '__main__':
    main()
