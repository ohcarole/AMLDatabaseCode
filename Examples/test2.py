import pandas as pd

# Create a Pandas dataframe from the data.
df = pd.DataFrame({'Data': [10, 20, 30, 20, 15, 30, 45]})
output = df.to_csv()
print (output)
pass