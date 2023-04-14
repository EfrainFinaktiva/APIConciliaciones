from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from db_utils import DB_Connection
from data_models import Insercion_Datos
import tabula
import pandas as pd
import os
import shutil

app = FastAPI()

# agregar cabeceras CORS
origins = [
    "http://localhost",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello Fucking World"}

@app.post("/uploadfile")
async def create_upload_file(file: UploadFile = File(...)):
    
    #name_file = file.filename
    name_file = await save_file_locally(file)
    
    #Validar si el archivo es PDF
    if not name_file.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="El archivo no tiene el formato correcto. Por favor suba un archivo PDF.")
    
    # Definir área de la tabla en la página 1
    area = [197.78, 19.32, 760.05, 591.54]

    try:
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
        insert_data_base(data_pg1, db_conn)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="El archivo no tiene tablas para procesar")
        
    
    # Crear una lista para almacenar los registros de Insercion_Datos del resto de las páginas
    data_resto = []
    
    # Definir área de la tabla en la página 2 en adelante
    area_pages = [11.90, 21.62, 751.66, 593.07]

    try:
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
        insert_data_base(data_resto, db_conn)
    except IndexError:
        raise HTTPException(status_code=400, detail="El archivo no tiene tablas para procesar")
    
    # Cerrar la conexión a la base de datos
    count = len(data_pg1) + len(data_resto)
    db_conn.insertar_trazabilidad(name_file, count)
    db_conn.desconectar()
    
    return "Archivo Procesado"

def insert_data_base(data_framework, db_conn):
    for data in data_framework:
        validation = db_conn.consultar_registro(data)
        
        if (validation):
            db_conn.actualizar_registro(data)
        else:
            db_conn.insertar_registro(data)
            
async def save_file_locally(file: UploadFile):
    file_name = file.filename
    file_location = f"files/{file_name}"
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return file_location