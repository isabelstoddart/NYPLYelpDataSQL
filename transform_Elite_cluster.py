import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
class EliteYear(beam.DoFn):
  def process(self, element):
    record = element
    user_id = record.get('user_id')
    elite = record.get('elite')
    # split by comma in elite
    elite_years = elite.split(',')
    # return dictionary of user, elite
    dict_list = []
    for i in elite_years:
      if i == "None":
        i = None
      dict = {'user_id': user_id, 'elite': i}
      dict_list.append(dict)
    return dict_list

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-elite-table',
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
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.User_'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply a ParDo to the PCollection 
    elite_pcoll = query_results | 'Normalize Elite' >> beam.ParDo(EliteYear())

    # write PCollection to a file
    elite_pcoll | 'Write File' >> WriteToText(DIR_PATH + 'elite_output.txt')
    
    # Create a new table in BigQuery with updated information from ParDo
    qualified_table_name = PROJECT_ID + ':workflow.Elite'
    table_schema = 'user_id:STRING,elite:INTEGER'
    
    elite_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
