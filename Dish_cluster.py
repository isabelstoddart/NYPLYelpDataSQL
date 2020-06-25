import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# RangeCalc takes the high price and low price, calculates the range, adds range as an item in the element dictionary
# and returns the updated element.
class RangeCalc(beam.DoFn):
  def process(self, element):
    record = element
    highest_price = record.get('highest_price')
    lowest_price = record.get('lowest_price')
    
    if(highest_price == None):
      highest_price = 0
    
    if(lowest_price == None):
      lowest_price = 0

    range = highest_price - lowest_price
    record['range'] = range
    return [record]
         
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-takes-table',
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
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset1.Dish'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply a ParDo to the PCollection 
    dish_pcoll = query_results | 'Calculate Range' >> beam.ParDo(RangeCalc())

    # write PCollection to a file
    dish_pcoll | 'Write File' >> WriteToText(DIR_PATH + 'dish_output.txt')
    
    # Create a new table in BigQuery with updated information from ParDo
    qualified_table_name = PROJECT_ID + ':dataset1.Dish1'
    table_schema = 'id:INTEGER,name:STRING,description:STRING,menus_appeared:INTEGER,times_appeared:INTEGER,first_appeared:INTEGER,last_appeared:INTEGER,lowest_price:FLOAT,highest_price:FLOAT,range:FLOAT'
    
    dish_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)) 
