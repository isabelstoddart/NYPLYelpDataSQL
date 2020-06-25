import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
class Oops(beam.DoFn):
  def process(self, element):
    record = element
    name= record.get('name')
    newname = name.strip('""')
    record['newname'] = newname
    return [record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-business-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    # Select data from Dish table in BigQuery 
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.Business'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply a ParDo to the PCollection 
    Bis_pcoll = query_results | 'Normalize Business' >> beam.ParDo(Oops())

    # write PCollection to a file
    Bis_pcoll | 'Write File' >> WriteToText(DIR_PATH + 'Bus_output.txt')
    
    # Create a new table in BigQuery with updated information from ParDo
    qualified_table_name = PROJECT_ID + ':workflow.Business'
    table_schema = 'business_id:STRING,name:STRING,neighborhood:STRING,address:STRING,city:STRING,state:STRING,postal_code:STRING,latitude:FLOAT,longitude:FLOAT,stars:FLOAT,review_count:INTEGER,is_open:INTEGER,categories:STRING,newname:STRING'
    
    Bis_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
