import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import csv
import boto3
import logging
import os

# Set up logging
logging.basicConfig(filename='logfile.log', level=logging.DEBUG)

# Set up S3 client
s3 = boto3.client('s3')

def download_file(url):
    """Downloads the file from the given URL and returns its content"""
    try:
        response = requests.get(url)
        content = response.content
        return content
    except Exception as e:
        logging.error(f"Error downloading file from URL: {url}\n{e}")
        raise e

def extract_zip(file_content):
    """Extracts the XML file from the given ZIP file content"""
    try:
        with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
            # Find the XML file whose file_type is DLTINS
            for name in zip_file.namelist():
                if 'DLTINS' in name and name.endswith('.xml'):
                    xml_content = zip_file.read(name)
                    return xml_content
            logging.error(f"No XML file found in the ZIP file.")
            raise ValueError("No XML file found in the ZIP file.")
    except Exception as e:
        logging.error(f"Error extracting XML from ZIP file.\n{e}")
        raise e

def parse_xml(xml_content):
    """Parses the given XML content and returns a list of dictionaries"""
    try:
        # Parse the XML content into an ElementTree object
        root = ET.fromstring(xml_content)
        
        # Find the first FinInstrmGnlAttrbts whose file_type is DLTINS
        for item in root.findall(".//{*}FinInstrmGnlAttrbts"):
            if item.find("{*}FileTp").text == 'DLTINS':
                data = {
                    'FinInstrmGnlAttrbts.Id': item.find("{*}Id").text,
                    'FinInstrmGnlAttrbts.FullNm': item.find("{*}FullNm").text,
                    'FinInstrmGnlAttrbts.ClssfctnTp': item.find("{*}ClssfctnTp").text,
                    'FinInstrmGnlAttrbts.CmmdtyDerivInd': item.find("{*}CmmdtyDerivInd").text,
                    'FinInstrmGnlAttrbts.NtnlCcy': item.find("{*}NtnlCcy").text,
                    'Issr': item.find("{*}Issr").text,
                }
                return [data]
        
        logging.error(f"No FinInstrmGnlAttrbts found in the XML file.")
        raise ValueError("No FinInstrmGnlAttrbts found in the XML file.")
    except Exception as e:
        logging.error(f"Error parsing XML.\n{e}")
        raise e

# Define function to write data to CSV
def write_csv(data):
    csv_file = 'output.csv'
    with open(csv_file, mode='w', newline='') as file:
        fieldnames = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    
    # Write CSV to S3 bucket
    s3 = boto3.resource('s3')
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    s3.Object(bucket_name, csv_file).upload_file(csv_file)
    
    # Remove CSV file from local directory
    os.remove(csv_file)
