import logging
import os
import re
import pandas as pd
import PyPDF2

from configs.tools.aws.s3 import AWSS3
from configs.tools.postgree import RDSPostgreSQLManager

logging.basicConfig(level=logging.INFO)

class PDFTExtExatractor:
    def __init__(self, pdf_file_path):
        self.pdf_file_path = pdf_file_path
        self.extracted_text = ""
        self.aws = AWSS3()

    def start(self):
        try:
            self.download_file()
            t = self.extrac_text()
            r = self.extract_operation()
            split = self.split_text_by_newline(r)
            df = self.text_to_dataframe(split)
            self.send_to_db(df, "pdf_text")
            print(df)
        except Exception as e:
            print(e)

    def download_file(self):
        bucket = os.getenv("AWS_BUCKET")
        if not os.path.exists("download"):
            os.mkdir("download", exist_ok=True)
        return self.aws.download_file_from_s3(bucket, self.pdf_file_path, f"download/{self.pdf_file_path}")
    
    def extract_text(self):
        with open(f"download/{self.pdf_file_path}", "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)

            extracted_text = ""

            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()

            return extracted_text
        
    def extract_operation(self, text):
        pattern = r"(C/V.*?)(?=\nPosição Ajuste)"

        result = re.search(pattern, text, re.DOTALL)

        if result:
            return result.group(1)
        else:
            print("Do not found")
            return
    
    def split_text_by_newline(self, text):
        if text:
            return text.split("\n")
        else:
            return []
        
    def text_to_dataframe(self, text):
        header = text[0].split()
        data = [
            line.split() for line in text[1:] if line
        ]

        df = pd.DataFrame(data, columns=header)

        return df
        

    def send_to_db(self, df, table_name):
        try:
            connection = RDSPostgreSQLManager().alchemy()
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logging.info("Has been sent succesfully to {table_name}")
            os.remove(f"download/{self.pdf_file_path}")

        except Exception as e:
            print(e)
