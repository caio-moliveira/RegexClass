import os 

import camelot
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

file_name = ""
path = os.path.abspath("path/{file_name}.pdf")

tables = camelot.read_pdf(
    path,
    flavor="stream",
    table_areas= ["72,560,492, 390"],
    strip_text=".\n",
    page="1-end"
)

camelot.plot(tables[0], kind="contour")

plt.show()

print(tables[0].df)
print("Waiting for work")
