import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
# DishPerPage calculates the average number of dishes per page for a particular menu based off of the page count of the menu
# and the dish count of the menu.
class DishPerPage(beam.DoFn):
  def process(self, element):
    record = element
    page_count = record.get('page_count')
    dish_count = record.get('dish_count')
    
    if page_count != None and dish_count != None:
      dishes_per_page = (dish_count / page_count)
    else:
      dishes_per_page = 0
   
    record['dishes_per_page'] = dishes_per_page
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
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset1.Menu2 limit 100'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    menu_pcoll = query_results | 'Calculate Dish Per Page' >> beam.ParDo(DishPerPage())

    # write PCollection to a file
    menu_pcoll | 'Write File' >> WriteToText('menu_output.txt')
    
    # Create a new table in BigQuery with updated information from ParDo
    qualified_table_name = PROJECT_ID + ':dataset1.Menu_1'
    table_schema = 'id:INTEGER,name:STRING,venue:STRING,place:STRING,event:STRING,physical_description:STRING,occasion:STRING,call_number:STRING,keywords:STRING,language:STRING,location:STRING,location_type:STRING,notes:STRING,currency:STRING,currency_symbol:STRING,sponsor:STRING,status:STRING,page_count:INTEGER,dish_count:INTEGER,date1:DATE,dishes_per_page:FLOAT'
    
    menu_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))   
