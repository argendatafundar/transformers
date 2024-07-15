from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def str_replace(df: DataFrame, col: str, pattern, replace: str, reg: bool = True):
    df[col] = df[col].str.replace(pattern, replace, regex=reg)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'nivel': 'indicador', 'sector': 'categoria', 'prop_educ': 'valor'}),
	sort_values(how='ascending', by=['categoria', 'indicador']),
	mutiplicar_por_escalar(col='valor', k=100),
	str_replace(col='indicador', pattern='^[a-z]\\. ', replace='', reg=True)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   nivel      21 non-null     object 
#   1   sector     21 non-null     object 
#   2   prop_educ  21 non-null     float64
#  
#  |    | nivel                  | sector   |   prop_educ |
#  |---:|:-----------------------|:---------|------------:|
#  |  0 | a. Primario incompleto | SBC      |  0.00194999 |
#  
#  ------------------------------
#  
#  rename_cols(map={'nivel': 'indicador', 'sector': 'categoria', 'prop_educ': 'valor'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  21 non-null     object 
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | indicador              | categoria   |      valor |
#  |---:|:-----------------------|:------------|-----------:|
#  |  0 | a. Primario incompleto | SBC         | 0.00194999 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria', 'indicador'])
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  21 non-null     object 
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | indicador           | categoria   |    valor |
#  |---:|:--------------------|:------------|---------:|
#  |  0 | Primario incompleto | SBC         | 0.194999 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  21 non-null     object 
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | indicador           | categoria   |    valor |
#  |---:|:--------------------|:------------|---------:|
#  |  0 | Primario incompleto | SBC         | 0.194999 |
#  
#  ------------------------------
#  
#  str_replace(col='indicador', pattern='^[a-z]\\. ', replace='', reg=True)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  21 non-null     object 
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | indicador           | categoria   |    valor |
#  |---:|:--------------------|:------------|---------:|
#  |  0 | Primario incompleto | SBC         | 0.194999 |
#  
#  ------------------------------
#  