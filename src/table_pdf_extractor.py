import logging
import os

import camelot
import pandas as pd
from unidecode import unidecode

from configs.tools.aws.s3 import AWSS3
from configs.tools.postgree import RDSPostgreSQLManager

logging.basicConfig(level=logging.INFO)


class PDFTableExtractor:
    def __init__(self, file_name, configs):
        self.file_name = file_name
        self.configs = configs
        self.aws = AWSS3()
        

    def start(self):
        try:
            self.download_file()
            main= self.get_table_data(self.configs["table_areas"], self.configs["columns"])
            main = self.sanitize_column_names(main)
            self.send_to_db(main, "pdf_table")
        except Exception as e:
            print(e)


    def get_table_data(self, table_areas, table_columns, fix=True):
        tables = camelot.read_pdf(
            f"download/{self.file_name}",
            flavor="stream",
            table_areas=table_areas,
            columns=table_columns,
            strip_text=".\n",
            pages="1-end"
        )
    
        table_content = [
            self.fix_header(page.df) if fix else page.df for page in tables
        ]


        #if there are more than one, concatenate them, if not, just show the one
        result = (
            pd.concat(table_content, ignore_index=True)
            if len(table_content) > 1
            else table_content[0]

        )


    @staticmethod
    def fix_header(df):
        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df.drop(df.column[0], axis=1)
        return df


    def download_file(self):
        bucket = os.getenv("AWS_BUCKET")
        if not os.path.exists("download"):
            os.makedirs("download", exist_ok=True)
        return self.aws.download_file_from_s3(
            bucket, self.file_name, f"download/{self.file_name}"
            )
    

    def sanitize_column_names(self, df):
        df.columns = df.columns.map(lambda x: unidecode(x))
        df.columns = df.columns.str.replace(" ", "_")
        df.columns = df.columns.str.replace(r"\W", "", regex=True)
        df.columns = df.columns.str.lower()
        return df
    
    def send_to_db(self, df, table_name):
        try:
            connection = RDSPostgreSQLManager().alchemy()
            df.to_sql(table_name, connection, if_exists="append", index=False)
            os.remove(f"download/{self.file_name}")
        except Exception as e:
            logging.error(e)

