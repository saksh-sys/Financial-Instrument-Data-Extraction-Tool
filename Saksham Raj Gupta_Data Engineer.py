import boto3
import logging
import os
import requests
import zipfile
import csv
import io
import xml.etree.ElementTree as ET

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# AWS S3 Configurations
S3_BUCKET = os.environ['S3_BUCKET']
S3_KEY = 'output.csv'

def download_extract_zip(download_url):
    response = requests.get(download_url)
    with open('temp.zip', 'wb') as f:
        f.write(response.content)
    with zipfile.ZipFile('temp.zip', 'r') as zip_ref:
        zip_ref.extractall('.')
    os.remove('temp.zip')
    logger.info('Extracted contents of zip file.')

def parse_xml_to_csv(xml_path):
    root = ET.parse(xml_path).getroot()
    ns = {'ns': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01'}
    fininstrms = root.findall(".//ns:FinInstrmGnlAttrbts", ns)
    with open('output.csv', 'w', newline='') as csvfile:
        fieldnames = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm',
                      'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                      'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for fininstrm in fininstrms:
            row = {}
            row['FinInstrmGnlAttrbts.Id'] = fininstrm.find('ns:Id', ns).text
            row['FinInstrmGnlAttrbts.FullNm'] = fininstrm.find('ns:FullNm', ns).text
            row['FinInstrmGnlAttrbts.ClssfctnTp'] = fininstrm.find('ns:ClssfctnTp', ns).text
            row['FinInstrmGnlAttrbts.CmmdtyDerivInd'] = fininstrm.find('ns:CmmdtyDerivInd', ns).text
            row['FinInstrmGnlAttrbts.NtnlCcy'] = fininstrm.find('ns:NtnlCcy', ns).text
            row['Issr'] = fininstrm.find('ns:Issr', ns).text
            writer.writerow(row)
    logger.info('Converted XML to CSV.')

def upload_file_to_s3(file_path, s3_key):
    s3 = boto3.client('s3')
    with open(file_path, 'rb') as f:
        s3.upload_fileobj(f, S3_BUCKET, s3_key)
    logger.info(f'Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}')

def lambda_handler(event, context):
    # Download xml from given URL
    xml_url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    download_xml = requests.get(xml_url)

  # Parse xml to find download link with file_type = DLTINS
    root = ET.fromstring(download_xml.content)
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    download_link = None

    for url in root.findall('.//ns:url', ns):
        file_type = url.find('ns:news', ns).find('ns:file_type', ns).text
        if file_type == 'DLTINS':
            download_link = url.find('ns:loc', ns).text
            break

    if download_link is None:
        raise ValueError("No download link found for file_type = DLTINS in the given XML")

    # Download zip file from the download link
    zip_file = requests.get(download_link)

    # Extract xml from the zip
    with zipfile.ZipFile(io.BytesIO(zip_file.content)) as zip_file:
        xml_filename = [name for name in zip_file.namelist() if name.endswith('.xml')][0]
        xml_data = zip_file.read(xml_filename)

    # Parse xml to create csv data
    root = ET.fromstring(xml_data)
    ns = {'ns': 'urn:iso:std:iso:20022:tech:xsd:head.001.001.01'}

    csv_data = "FinInstrmGnlAttrbts.Id,FinInstrmGnlAttrbts.FullNm,FinInstrmGnlAttrbts.ClssfctnTp,FinInstrmGnlAttrbts.CmmdtyDerivInd,FinInstrmGnlAttrbts.NtnlCcy,Issr\n"

    for instrmt in root.findall('.//ns:FinInstrmGnlAttrbts', ns):
        instrmt_id = instrmt.find('ns:Id', ns).text
        full_name = instrmt.find('ns:FullNm', ns).text
        clssfctn_tp = instrmt.find('ns:ClssfctnTp', ns).text
        cmmdty_deriv_ind = instrmt.find('ns:CmmdtyDerivInd', ns).text
        ntnl_ccy = instrmt.find('ns:NtnlCcy', ns).text
        issr = instrmt.find('ns:Issr', ns).text

        csv_data += f"{instrmt_id},{full_name},{clssfctn_tp},{cmmdty_deriv_ind},{ntnl_ccy},{issr}\n"

    # Write csv data to file
    with io.StringIO(csv_data) as csv_file:
        s3 = boto3.client('s3')
        s3.put_object(Body=csv_file.getvalue().encode('utf-8'), Bucket='saksh-sys', Key='Result.csv')

    return {
        'statusCode': 200,
        'body': 'CSV file successfully uploaded to S3'
    }