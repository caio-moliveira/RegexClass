import time
from datetime import datetime


from configs.tools.queue import PDFSQLListenner
from table_pdf_extractor import PDFTableExtractor
from text_pdf_extractor import PDFTExtExatractor
from configs.rules import rules_dict





if __name__ =="__main__":

    configs= rules_dict["jornada"]
    PDFTableExtractor(".pdf", configs).start()