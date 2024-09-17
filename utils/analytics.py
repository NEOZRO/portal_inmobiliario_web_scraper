import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'

def remove_outliers(self, column_name):
    """ removedor de outliers para columna especificada"""
    Q1 = self[column_name].quantile(0.25)
    Q3 = self[column_name].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    condition = (self[column_name] >= lower_bound) & (self[column_name] <= upper_bound)
    return self.loc[condition]

pd.DataFrame.remove_outliers = remove_outliers

class Analytics():
    """ class to process database results """
    def __init__(self):
        self.df_analysis = None
        self.analysis_results = None


    def generate_df_caprates(self):

        for tipo in ["casa","departamento"]:
            print(f"analizando {tipo}")
            df_arriendo = self.df_analysis.query(f"tipo_operacion=='arriendo' and tipo_inmueble=='{tipo}'")


            self.analysis_results[f"df_arriendo_{tipo}"] = df_arriendo
            results_precio_arriendo = {}
            for n_dorm in pd.Series(df_arriendo["n_dormitorios"].unique()).sort_values().values:

                clean_tipology_df = df_arriendo.query(f"n_dormitorios=={n_dorm}").remove_outliers('Price_UF')
                precio_arriendo = int(clean_tipology_df.Price.quantile(0.1))
                results_precio_arriendo[n_dorm] = precio_arriendo
                print(f"con {n_dorm} dormitorios, se puede arrendar en aproximadamente {precio_arriendo}, analizado desde {len(clean_tipology_df)} propiedades")


            list_results = []
            for n_dorm in pd.Series(df_arriendo["n_dormitorios"].unique()).sort_values().values:
                clean_tipology_df = df_arriendo.query(f"n_dormitorios=={n_dorm}").remove_outliers('superficie_util')
                list_results.append(clean_tipology_df)


            df_price_wo_outliers = pd.concat(list_results)
            df_price_wo_outliers["precio_m2"] = df_price_wo_outliers.Price_UF/df_price_wo_outliers.superficie_util
            promedio_zona = int(np.mean(df_price_wo_outliers.Price/df_price_wo_outliers.superficie_util))
            promedio_zona_UF = np.round(np.mean(df_price_wo_outliers.Price_UF/df_price_wo_outliers.superficie_util),2)

            print(f"precio/m2 de la zona es {promedio_zona} CLP , {promedio_zona_UF} UF")

            df_venta = self.df_analysis.query(f"tipo_operacion=='venta' and tipo_inmueble=='{tipo}'")

            df_venta["caprate_bruto"]=np.NaN
            for n_dorm in pd.Series(df_venta["n_dormitorios"].unique()).sort_values().values:
                if n_dorm in results_precio_arriendo.keys():
                    df_venta.loc[df_venta["n_dormitorios"]==n_dorm, "caprate_bruto" ] = results_precio_arriendo[n_dorm] * 12 / df_venta.query(f"n_dormitorios=={n_dorm}").Price

            df_venta.caprate_bruto = df_venta.caprate_bruto.astype(float)

            self.analysis_results[f"df_caprate_{tipo}"] = df_venta
            print("-------------------------------------")
