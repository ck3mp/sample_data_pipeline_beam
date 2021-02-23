import argparse
import csv
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


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


def create_output(s3_bucket):
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    s3_prefix = f's3://{s3_bucket}/PROCESSED/extracted_year={year}/extracted_month={month}/extracted_day={day}/{now.isoformat()}_output'
    return s3_prefix


def main():
    # If we give a path the use it as output. Else us S3.
    if args.output:
        out_key = args.output
    else:
        # Use S3 Data Lake Structuring for Athena/Glue for future cataloging etc.
        out_key = create_output(args.s3_bucket)

    options = PipelineOptions()

    # Use context handler so our pipeline can be closed automatically and means execution is run automatically
    with beam.Pipeline(options=options) as p:
        # Load the CVS, Parse it into Python Objects, Turn to JSON Strings, and Write to File
        _ = (
                p
                | 'Read CSV file' >> beam.io.ReadFromText(args.input)
                | 'Convert file to PyObjects' >> beam.Map(parse_csv_file)
                | 'Dump to List of JSON Lines' >> beam.ParDo(JSONDump())
                | 'Write the final transformed data to file' >> beam.io.WriteToText(out_key)
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        help='CSV input file',
                        default=
                        '/Users/chris/Development/sample_data_pipeline_beam/data/pp-monthly-update-new-version.csv'
                        )
    parser.add_argument('--output',
                        help='CSV input file',
                        default=None
                        )
    parser.add_argument('--s3_bucket',
                        help='S3 BUCKET',
                        default='dev'
                        )
    args, beam_args = parser.parse_known_args()

    main()
