from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def agregar_anio(df: pd.DataFrame) -> pd.DataFrame:
    def calcular_anio(row):
        if row["medida_fecha"] == "año":
            return int(row["valor_fecha"])
        elif row["medida_fecha"] == "período":
            try:
                inicio, fin = row["valor_fecha"].split("-")
                return int((int(inicio) + int(fin)) / 2)
            except ValueError:
                return None
        return None

    df = df.copy()
    df["anio"] = df.apply(calcular_anio, axis=1)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	agregar_anio(),
	query(condition="(anio >= 1950 and fuente =='World Population Prospects (UN)') or (anio < 1950 and fuente == 'INDEC')"),
	sort_values(how='ascending', by=['anio']),
	drop_col(col=['fuente', 'medida_fecha', 'valor_fecha'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   valor_fecha        85 non-null     object 
#   1   medida_fecha       85 non-null     object 
#   2   exp_vida_al_nacer  85 non-null     float64
#   3   fuente             85 non-null     object 
#  
#  |    |   valor_fecha | medida_fecha   |   exp_vida_al_nacer | fuente                          |
#  |---:|--------------:|:---------------|--------------------:|:--------------------------------|
#  |  0 |          1950 | año            |             61.2746 | World Population Prospects (UN) |
#  
#  ------------------------------
#  
#  agregar_anio()
#  RangeIndex: 85 entries, 0 to 84
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   valor_fecha        85 non-null     object 
#   1   medida_fecha       85 non-null     object 
#   2   exp_vida_al_nacer  85 non-null     float64
#   3   fuente             85 non-null     object 
#   4   anio               85 non-null     int64  
#  
#  |    |   valor_fecha | medida_fecha   |   exp_vida_al_nacer | fuente                          |   anio |
#  |---:|--------------:|:---------------|--------------------:|:--------------------------------|-------:|
#  |  0 |          1950 | año            |             61.2746 | World Population Prospects (UN) |   1950 |
#  
#  ------------------------------
#  
#  query(condition="(anio >= 1950 and fuente =='World Population Prospects (UN)') or (anio < 1950 and fuente == 'INDEC')")
#  Index: 80 entries, 0 to 79
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   valor_fecha        80 non-null     object 
#   1   medida_fecha       80 non-null     object 
#   2   exp_vida_al_nacer  80 non-null     float64
#   3   fuente             80 non-null     object 
#   4   anio               80 non-null     int64  
#  
#  |    |   valor_fecha | medida_fecha   |   exp_vida_al_nacer | fuente                          |   anio |
#  |---:|--------------:|:---------------|--------------------:|:--------------------------------|-------:|
#  |  0 |          1950 | año            |             61.2746 | World Population Prospects (UN) |   1950 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   valor_fecha        80 non-null     object 
#   1   medida_fecha       80 non-null     object 
#   2   exp_vida_al_nacer  80 non-null     float64
#   3   fuente             80 non-null     object 
#   4   anio               80 non-null     int64  
#  
#  |    | valor_fecha   | medida_fecha   |   exp_vida_al_nacer | fuente   |   anio |
#  |---:|:--------------|:---------------|--------------------:|:---------|-------:|
#  |  0 | 1869-1895     | período        |               32.86 | INDEC    |   1882 |
#  
#  ------------------------------
#  
#  drop_col(col=['fuente', 'medida_fecha', 'valor_fecha'], axis=1)
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   exp_vida_al_nacer  80 non-null     float64
#   1   anio               80 non-null     int64  
#  
#  |    |   exp_vida_al_nacer |   anio |
#  |---:|--------------------:|-------:|
#  |  0 |               32.86 |   1882 |
#  
#  ------------------------------
#  