import mysql.connector
import decimal
from decimal import Decimal


class DB_Connection:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conexion = None
        self.cursor = None

    def conectar(self):
        self.conexion = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.conexion.cursor()

    def desconectar(self):
        if self.cursor:
            self.cursor.close()
        if self.conexion:
            self.conexion.close()
            
    def consultar_registro(self, registro):
        self.conectar()
        
        sql = """
            SELECT COUNT(*) FROM registros WHERE fecha = %s AND valor = %s
        """
        # Asignamos valores para la consulta a la base de datos.
        valores = (
            registro.fecha,
            Decimal(registro.valor.replace(",", "")) # convertir formato de valor
        )
        
        self.cursor.execute(sql, valores)
        results = self.cursor.fetchone()[0]
        self.conexion.commit()
        self.desconectar()
        
        # Retornamos el resultado true o false dependiendo la evaluacion dada
        return results > 0

    def insertar_registro(self, registro):
        self.conectar()
        
        sql = """
            INSERT INTO registros (fecha, descripcion, sucursal_canal, referencia_1, referencia_2, documento, valor)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        #Validamos el dato de nit, para quitar el 0 o que si esta vacio sea Null en base de datos.
        if registro.referencia_1 is not None:
            try:
                ref_1 = int(float(registro.referencia_1))
            except ValueError:
                ref_1 = None
        else:
            ref_1 = ""

        if registro.referencia_2 is not None:
            try:
                ref_2 = int(float(registro.referencia_2))
            except ValueError:
                ref_2 = None
        else:
            ref_2 = ""
        
        if registro.documento is not None:
            try:
                document = int(float(registro.documento))
            except ValueError:
                document = None
        else:
            document = None
        
        #Asignamos valores para insertar en base de datos.        
        valores = (
            registro.fecha,
            registro.descripcion,
            registro.sucursal_canal,
            ref_1,
            ref_2,
            document,
            Decimal(registro.valor.replace(",", "")) # convertir formato de valor
        )

        self.cursor.execute(sql, valores)
        self.conexion.commit()
        self.desconectar()
        
    def actualizar_registro(self, registro):
        self.conectar()
        
        sql = """
            UPDATE registros 
            SET descripcion = %s, 
                sucursal_canal = %s, 
                referencia_1 = %s, 
                referencia_2 = %s, 
                documento = %s 
            WHERE fecha = %s AND valor = %s
        """
        #Validamos el dato de nit, para quitar el 0 o que si esta vacio sea Null en base de datos.
        if registro.referencia_1 is not None:
            try:
                ref_1 = int(float(registro.referencia_1))
            except ValueError:
                ref_1 = None
        else:
            ref_1 = ""

        if registro.referencia_2 is not None:
            try:
                ref_2 = int(float(registro.referencia_2))
            except ValueError:
                ref_2 = None
        else:
            ref_2 = ""
        
        if registro.documento is not None:
            try:
                document = int(float(registro.documento))
            except ValueError:
                document = None
        else:
            document = None
        
        #Asignamos valores para insertar en base de datos.        
        valores = (
            registro.fecha,
            registro.descripcion,
            registro.sucursal_canal,
            ref_1,
            ref_2,
            document,
            Decimal(registro.valor.replace(",", "")) # convertir formato de valor
        )

        self.cursor.execute(sql, valores)
        self.conexion.commit()
        self.desconectar()
