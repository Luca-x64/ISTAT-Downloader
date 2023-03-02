import pandas as pa

excel_path="xlsm/new_dimension.xlsm"
resourceID = "22_289"
df = pa.read_excel(excel_path,sheet_name="Transcodifica Dimensioni",index_col=20,)

res = df[df["DF"]=="22_289"]
res = res.reset_index()

new_position=[]
for index,row in res.iterrows():
    new_position.append(row["POS_NEW"])
print(new_position)