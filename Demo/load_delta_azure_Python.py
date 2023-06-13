import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import re ,shutil
from urllib.request import urlopen
import os
import adlfs 
from deltalake.writer import write_deltalake



AZURE_STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME') 
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY') 
table_path = os.environ.get('table_path')

storage_options = {
"account_name": AZURE_STORAGE_ACCOUNT_NAME ,
"azure_storage_tenant_id" : os.environ.get('account_id') ,
"AZURE_STORAGE_ACCOUNT_KEY":  AZURE_STORAGE_ACCOUNT_KEY
}

def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def load(request):   
    fs = adlfs.AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT_NAME, account_key=AZURE_STORAGE_ACCOUNT_KEY )
    appended_data = []
    url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist1 = pattern.findall(result)
    filelist_unique = dict.fromkeys(filelist1)
    filelist_sorted=sorted(filelist_unique, reverse=True)
    filelist = filelist_sorted[:1000]
    try:
        df = ds.dataset(table_path +"/aemo/log/scada/part-0.parquet",filesystem=fs).to_table().to_pandas()
    except:
        df=pd.DataFrame(columns=['file']) 
         
    file_loaded= df['file'].unique()
    #print (df)

    current = file_loaded.tolist()
    #print(current)

    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    print(str(len(files_to_upload)) + ' New File Loaded')
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
            df=df.dropna(how='all') #drop na
            df['SETTLEMENTDATE']= pd.to_datetime(df['SETTLEMENTDATE'])
            df['Date'] = df['SETTLEMENTDATE'].dt.date
            df['file'] = x
            appended_data.append(df)
            # see pd.concat documentation for more info
      appended_data = pd.concat(appended_data)
      existing_file = pd.DataFrame( file_loaded)
      new_file = pd.DataFrame(  appended_data['file'].unique())
      log = pd.concat ([new_file,existing_file], ignore_index=True)
      #print(log)
      log.rename(columns={0: 'file'}, inplace=True)
      
      log_tb=pa.Table.from_pandas(log,preserve_index=False)
      #print(log_tb)
      log_schema = pa.schema([pa.field('file', pa.string())])
      log_tb=log_tb.cast(target_schema=log_schema)
      
      tb=pa.Table.from_pandas(appended_data,preserve_index=False)
      my_schema = pa.schema([
                      pa.field('SETTLEMENTDATE', pa.timestamp('us')),
                      pa.field('DUID', pa.string()),
                      pa.field('SCADAVALUE', pa.float64()),
                      pa.field('Date', pa.date32()),
                      pa.field('file', pa.string())
                      ]
                                                       )
      xx=tb.cast(target_schema=my_schema)
      write_deltalake(table_or_uri='az://aemo/scada', data=xx, partition_by=["Date"],mode="append", storage_options=storage_options)   
      ds.write_dataset(log_tb,table_path +"/aemo/log/scada",filesystem=fs, format="parquet" ,existing_data_behavior="overwrite_or_ignore")
      return "done"
