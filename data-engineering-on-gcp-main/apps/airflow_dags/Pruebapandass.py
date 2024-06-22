import pandas as pd

df = pd.DataFrame(data=[[0, '10/11/12']],
                  columns=['int_column', 'date_column'])

data= df['int_column'] 

print(data)