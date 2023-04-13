from fastapi import FastAPI, File, UploadFile, HTTPException
from typing import List
import tabula
import pandas as pd
from db_utils import DB_Connection
from data_models import Insercion_Datos

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello Fucking World"}

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    name_file = file.filename
    
    #Validar si el archivo es PDF
    if not name_file.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="El archivo no tiene el formato correcto. Por favor suba un archivo PDF.")
    
    # Definir área de la tabla en la página 1
    area = [197.78, 19.32, 760.05, 591.54]

    # Leer la tabla de la página 1 con las condiciones especificadas
    df_pg1 = tabula.read_pdf(name_file, pages=1, area=area, guess=False)[0]
    df_pg1.fillna('', inplace=True)
    # Crear una lista para almacenar los registros de Insercion_Datos de la página 1
    data_pg1 = []
    for row in df_pg1.itertuples(index=False):
        data_pg1.append(Insercion_Datos(
            fecha=row[0],
            descripcion=row[1],
            sucursal_canal=row[2],
            referencia_1=row[3],
            referencia_2=row[4],
            documento=row[5],
            valor=row[6]
        ))

    # Conectar con la base de datos
    db_conn = DB_Connection("127.0.0.1", "root", "root", "pagos")

    # Insertar los registros de la página 1 a la base de datos
    for data in data_pg1:
        validation = db_conn.consultar_registro(data)
        
        if (validation):
            db_conn.actualizar_registro(data)
        else:
            db_conn.insertar_registro(data)

    # Crear una lista para almacenar los registros de Insercion_Datos del resto de las páginas
    data_resto = []

    area_pages = [11.90, 21.62, 751.66, 593.07]

    # Extraer los datos de las páginas restantes y agregarlos a la lista
    for i in range(2, len(tabula.read_pdf(name_file, pages="all", stream=True, area=area_pages, guess=False))+1):
        df = tabula.read_pdf(name_file, pages=i, pandas_options={"header": None})[0]
        df.fillna('', inplace=True)
        for row in df.itertuples(index=False):
            data_resto.append(Insercion_Datos(
                fecha=row[0],
                descripcion=row[1],
                sucursal_canal=row[2],
                referencia_1=row[3],
                referencia_2=row[4],
                documento=None,
                valor=row[5]
            ))

    # Insertar los registros del resto de las páginas a la base de datos
    for data in data_resto:
        validation = db_conn.consultar_registro(data)
        
        if (validation):
            db_conn.actualizar_registro(data)
        else:
            db_conn.insertar_registro(data)

    # Cerrar la conexión a la base de datos
    db_conn.desconectar()
    
    return "Archivo Procesado"

