from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def fecha_to_trimestre(df, col, trimestre_map, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}-1'):
    from re import findall

    def get_trimestre(fecha):
        groups = findall(input_format_pattern, fecha)[0]
        trimester_index = extractor.index('trimester')
        year_index = extractor.index('year')
        trimester = groups[trimester_index]
        year = groups[year_index]
        return trimestre_format.format(year=year, trimester=trimestre_map[trimester])

    df[col] = df[col].map(get_trimestre)

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	fecha_to_trimestre(col='fecha', trimestre_map={'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'VI': 4}, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}'),
	rename_cols(map={'fecha': 'aniotrim', 'tasa_feminizacion': 'valor'}),
	query(condition="aniotrim.str.endswith('-1')")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   fecha              107 non-null    object 
#   1   tasa_feminizacion  107 non-null    float64
#  
#  |    | fecha   |   tasa_feminizacion |
#  |---:|:--------|--------------------:|
#  |  0 | I_1996  |             6.62948 |
#  
#  ------------------------------
#  
#  fecha_to_trimestre(col='fecha', trimestre_map={'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'VI': 4}, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}')
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   fecha              107 non-null    object 
#   1   tasa_feminizacion  107 non-null    float64
#  
#  |    | fecha   |   tasa_feminizacion |
#  |---:|:--------|--------------------:|
#  |  0 | 1996-1  |             6.62948 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'aniotrim', 'tasa_feminizacion': 'valor'})
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  107 non-null    object 
#   1   valor     107 non-null    float64
#  
#  |    | aniotrim   |   valor |
#  |---:|:-----------|--------:|
#  |  0 | 1996-1     | 6.62948 |
#  
#  ------------------------------
#  
#  query(condition="aniotrim.str.endswith('-1')")
#  Index: 27 entries, 0 to 104
#  Data columns (total 2 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   aniotrim  27 non-null     object 
#   1   valor     27 non-null     float64
#  
#  |    | aniotrim   |   valor |
#  |---:|:-----------|--------:|
#  |  0 | 1996-1     | 6.62948 |
#  
#  ------------------------------
#  