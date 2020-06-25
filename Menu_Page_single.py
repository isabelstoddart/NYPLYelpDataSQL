import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# OrientationCalc determines whether a menu page is oriented vertically or horizontally based off of its height and
# width dimensions
class OrientationCalc(beam.DoFn):
  def process(self, element):
    record = element
    full_height = record.get('full_height')
    full_width = record.get('full_width')
    
    if full_height != None and full_width != None:
      if(full_height > full_width):
        orientation = 'vertical'
      elif(full_height < full_width):
        orientation = 'horizontal'
      else:
        orientation = 'N/A'
    else:
      orientation = 'N/A'
   
    record['orientation'] = orientation
    return [record]
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    # Select data from Dish table in BigQuery 
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset1.Menu_Page limit 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    page_pcoll = query_results | 'Calculate Orientation' >> beam.ParDo(OrientationCalc())

    # write PCollection to a file
    page_pcoll | 'Write File' >> WriteToText('page_output.txt')
    
    # Create a new table in BigQuery with updated information from ParDo
    qualified_table_name = PROJECT_ID + ':dataset1.Menu_Page1'
    table_schema = 'id:INTEGER,menu_id:INTEGER,page_number:INTEGER,image_id:STRING,full_height:INTEGER,full_width:INTEGER,uuid:STRING,orientation:STRING'
    
    page_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))   
