from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'region': 'categoria'}),
	drop_col(col='porcentaje', axis=1),
	query(condition='categoria != "Total"'),
	sort_values(how='ascending', by=['categoria', 'indicador'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region        36 non-null     object 
#   1   tipo_energia  36 non-null     object 
#   2   valor_en_mw   36 non-null     int64  
#   3   porcentaje    36 non-null     float64
#  
#  |    | region                           | tipo_energia   |   valor_en_mw |   porcentaje |
#  |---:|:---------------------------------|:---------------|--------------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía     |            48 |      3.21932 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'region': 'categoria'})
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   categoria   36 non-null     object 
#   1   indicador   36 non-null     object 
#   2   valor       36 non-null     int64  
#   3   porcentaje  36 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |   porcentaje |
#  |---:|:---------------------------------|:------------|--------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |      3.21932 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  36 non-null     object
#   1   indicador  36 non-null     object
#   2   valor      36 non-null     int64 
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |
#  
#  ------------------------------
#  
#  query(condition='categoria != "Total"')
#  Index: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  32 non-null     object
#   1   indicador  32 non-null     object
#   2   valor      32 non-null     int64 
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria', 'indicador'])
#  Index: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  32 non-null     object
#   1   indicador  32 non-null     object
#   2   valor      32 non-null     int64 
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |
#  
#  ------------------------------
#  