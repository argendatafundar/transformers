from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'region': 'categoria'}),
	drop_col(col='porcentaje', axis=1),
	query(condition='categoria != "Total" & valor != 0'),
	sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Carbón': 1, 'Petróleo': 2, 'Gas natural': 3, 'Hidro': 4, 'Nuclear': 5, 'Eólica': 6, 'Fotovoltaica': 7, 'Biocombustibles': 8, 'Otras renovables': 9}, prefix=['categoria'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region        36 non-null     object 
#   1   tipo_energia  36 non-null     object 
#   2   valor_en_mw   36 non-null     float64
#   3   porcentaje    36 non-null     float64
#  
#  |    | region                           | tipo_energia   |   valor_en_mw |   porcentaje |
#  |---:|:---------------------------------|:---------------|--------------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía     |        41.458 |      2.09664 |
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
#   2   valor       36 non-null     float64
#   3   porcentaje  36 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |   porcentaje |
#  |---:|:---------------------------------|:------------|--------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |  41.458 |      2.09664 |
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
#   2   valor      36 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |  41.458 |
#  
#  ------------------------------
#  
#  query(condition='categoria != "Total" & valor != 0')
#  Index: 21 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  21 non-null     object 
#   1   indicador  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |  41.458 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Carbón': 1, 'Petróleo': 2, 'Gas natural': 3, 'Hidro': 4, 'Nuclear': 5, 'Eólica': 6, 'Fotovoltaica': 7, 'Biocombustibles': 8, 'Otras renovables': 9}, prefix=['categoria'], suffix=[])
#  Index: 21 entries, 1 to 29
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  21 non-null     object 
#   1   indicador  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  1 | CABA y Provincia de Buenos Aires | Eólica      |  1935.9 |
#  
#  ------------------------------
#  