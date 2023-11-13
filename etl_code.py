# extract data from CSV, JSON, and XML formats
# The xml library can be used to parse the information from an .xml file format, The .csv and .json file formats can be read using the pandas library
# use the pandas library to create a data frame format that will store the extracted data from any file

# pip install pandas

# use the glob library to access the file format information, to call the correct function for data extraction
# use the datetime package to get date and time information at the point of logging
# import the ElementTree function from the xml.etree library to parse the data from an XML file format
import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

# log_file.txt: to stores all the logs
# transformed_data.csv: to store the final output data that I can load to a database
log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

##############################################   Extraction   ##############################################
# extract data from a CSV file
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

# extract data from a json file
def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe

# extract data from a xml file
def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return dataframe 

# write a function extract which uses the glob library to identify the filetype, to call the relevant function
def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data 
     
    # process all csv files 
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 

    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data



##############################################   Transformation   ##############################################
def transform(data): 
    # Convert inches to meters and round off to 2 decimals 
    # 1 inch = 0.0254 meters
    data['height'] = round(data.height * 0.0254,2) 
 
    # Convert pounds to kg and round off to 2 decimals 
    # 1 pound = 0.45359237 kg
    data['weight'] = round(data.weight * 0.45359237,2) 
    
    return data 



##############################################   Loading and Logging   ##############################################
# load the transformed data to a CSV file to load it to the database
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

# implement the logging operation to record the progress of the different operations
# by recording a message along with its timestamp in the log_file
# log_progress(): accepts the log message as the argument
# The function captures the current date and time using the datetime function from the datetime library
# The use of this function requires the definition of a date-time format &convert the timestamp to a string format using the strftime attribute
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')



####################################   Testing ETL operations and log progress  ####################################
# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 