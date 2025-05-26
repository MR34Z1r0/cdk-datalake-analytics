import pandas as pd

a = '2024-07-26-18-44-15-04446cd5c032036b82d8cfe6d402449181a04507.invoices_in_waybills.json'

dat = a[:19]
b = pd.to_datetime(dat, format='%Y-%m-%d-%H-%M-%S')

print(b)