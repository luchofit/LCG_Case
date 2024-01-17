import psycopg2
import psycopg2.extras
import pandas as pd
import os 
from pathlib import Path 

#Selecciono todas mis tablas para después ponerla en un df
Statement1 = "SELECT * FROM BD"
Statement2 = "SELECT * FROM bd_Original"
Statement3 = "SELECT * FROM Departamento"
Statement4 = "SELECT * FROM Vendedor"
root_dir = Path(".").resolve().parent
try:
    connection = psycopg2.connect(
        host= 'localhost',
        dbname = "proyectoX",
        user= "postgres" ,
        password = 'PRUEBA123',
        port = 5433
    )

except Exception as ex:
    print("Ocurrió un error al conectar a PostgreSQL: ", ex)
# finally:
#   if cursor is not None:
#       cursor.close()
#   if connection is not None:  
#       connection.close()
#   print("conexión finalizada")

def get_data():
    try:
        connection = psycopg2.connect(
            host='localhost',
            dbname="LCG_Case",
            user="postgres",
            password='Lmop-1549f',
            port=5432
        )
        print("Conexión exitosa")
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(Statement1.encode('LATIN9'))
        BD = cursor.fetchall()
        cursor2 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor2.execute(Statement2.encode('LATIN9'))
        bd_Original = cursor2.fetchall()
        cursor3 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor3.execute(Statement3.encode('LATIN9'))
        Departamentos = cursor3.fetchall()
        cursor4 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor4.execute(Statement4.encode('LATIN9'))
        Vendedor = cursor4.fetchall()
        new_BD = pd.DataFrame(BD)
        new_BD.columns =["Fecha","Número de Vendedor","Nom_Completo_Vendedor","Número de cliente","Tipo","Departamento - Clave","Departamento","Familia - Clave","Familia","Ventas Netas (Q)","Costo"]
        new_bd_Original = pd.DataFrame(bd_Original)
        new_bd_Original.columns =["Depto","Departamento","Fam","Familia","anioretail","mesretail","ventasnetas","costos","Utilidad","% Margen"]
        new_Departamento = pd.DataFrame(Departamentos)
        new_Departamento.columns =["Departamento - Clave","Departamento"]
        new_Vendedor = pd.DataFrame(Vendedor)
        new_Vendedor.columns =["No. Vendedor","Nombre","Apellido"]
    except Exception as ex:
        print("Ocurrió un error al conectar a PostgreSQL: ", ex)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
            print("Conexión finalizada")
    return new_BD, new_bd_Original, new_Departamento, new_Vendedor

def generate_report(new_BD, new_bd_Original, new_Departamento, new_Vendedor):
    new_BD['Fecha'] = pd.to_datetime(new_BD['Fecha'])
    new_BD['Año'] = new_BD['Fecha'].dt.year
    new_BD['Mes'] = new_BD['Fecha'].dt.month
    new_Vendedor['No. Vendedor'] = new_Vendedor['No. Vendedor'].astype('string')
    merged_df = pd.merge(new_BD, new_Vendedor, left_on='Número de Vendedor', right_on='No. Vendedor', how='left')
    merged_df['Nombre_Completo'] = merged_df['Nombre'] + ' ' + merged_df['Apellido']
    new_Departamento['Departamento - Clave'] = new_Departamento['Departamento - Clave'].astype('string')
    merged_df = merged_df.merge(new_Departamento[['Departamento - Clave', 'Departamento']],
                            how='left',
                            left_on='Departamento - Clave',
                            right_on='Departamento - Clave',
                            suffixes=('_original', '_nuevo'))
    merged_df['Departamento final'] = merged_df.apply(lambda row: row['Departamento_original'] if pd.notna(row['Departamento_original']) else row['Departamento_nuevo'], axis=1)
    merged_df = merged_df.merge(new_Departamento[['Departamento - Clave', 'Departamento']],
                            how='left',
                            left_on='Departamento final',
                            right_on='Departamento',
                            suffixes=('_original', '_nuevo'))
    merged_df['Ventas Netas (Q)'] = merged_df['Ventas Netas (Q)'].astype(float)
    merged_df['Costo'] = merged_df['Costo'].astype(float)
    merged_df['Ventas Netas (Q)'] = merged_df['Ventas Netas (Q)'].apply(lambda x: abs(x) if x is not None else None)
    merged_df['Costo'] = merged_df['Costo'].apply(lambda x: -abs(x) if x is not None else None)
    merged_df['Ventas en USD'] = merged_df['Ventas Netas (Q)'].apply(lambda x: float(x) / 7.5 if x is not None else None)
    merged_df['Costo en USD'] = merged_df['Costo'].apply(lambda x: float(x) / 7.5 if x is not None else None)
    merged_df['Utilidad en USD'] = merged_df['Ventas en USD'] + merged_df['Costo en USD']
    merged_df['Margen'] = merged_df['Utilidad en USD'] / merged_df['Ventas en USD']
    Final_df = merged_df[['Fecha','Año','Mes','No. Vendedor','Nombre_Completo','Número de cliente','Tipo','Departamento - Clave_nuevo','Departamento final','Familia - Clave','Familia','Ventas Netas (Q)','Costo','Ventas en USD', 'Costo en USD', 'Utilidad en USD', 'Margen']]
    Final_df[['Ventas Netas (Q)', 'Costo', 'Ventas en USD', 'Costo en USD', 'Utilidad en USD', 'Margen']] = \
        Final_df[['Ventas Netas (Q)', 'Costo', 'Ventas en USD', 'Costo en USD', 'Utilidad en USD', 'Margen']].round(2)
    return Final_df


def save_date(Final_df):
    out_name = "reporte.csv"  
    out_path = os.path.join(root_dir,"Datos","Processed", out_name)
    Final_df.to_csv(out_path,encoding='latin9', index=False)

def main():
    new_BD, new_bd_Original, new_Departamento, new_Vendedor = get_data()
    Final_df = generate_report(new_BD, new_bd_Original, new_Departamento, new_Vendedor)
    save_date(Final_df)

if __name__ == "__main__":
    main()