from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	sort_values(how='descending', by=['ratio_emisivo_ponderado'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 191 entries, 0 to 190
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   geocodigoFundar          191 non-null    object 
#   1   geonombreFundar          191 non-null    object 
#   2   ratio_emisivo_ponderado  191 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ratio_emisivo_ponderado |
#  |---:|:------------------|:------------------|--------------------------:|
#  |  0 | ABW               | Aruba             |                   16.7893 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by=['ratio_emisivo_ponderado'])
#  RangeIndex: 191 entries, 0 to 190
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   geocodigoFundar          191 non-null    object 
#   1   geonombreFundar          191 non-null    object 
#   2   ratio_emisivo_ponderado  191 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ratio_emisivo_ponderado |
#  |---:|:------------------|:------------------|--------------------------:|
#  |  0 | ALB               | Albania           |                   22.0768 |
#  
#  ------------------------------
#  