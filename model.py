import psycopg2
import time
import random
import string
from datetime import datetime, timedelta
import logging

class Model:
    def __init__(self):
        # Налаштування логування
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.conn = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='Mamchyts',
            host='localhost',
            port=5432
        )

    def execute_sql(self, sql):
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        with self.conn.cursor() as c:
            c.execute(sql)
            self.conn.commit()

    def get_all_tables(self):
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        with self.conn.cursor() as c:
            c.execute(sql)
            tables = [table[0] for table in c.fetchall()]
        return tables

    def get_all_columns(self, table_name):
        sql = "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position"
        self.logger.debug(f"Executing SQL: {sql} with parameter: {table_name}")  # Логуємо SQL-запит і параметри
        with self.conn.cursor() as c:
            c.execute(sql, (table_name,))
            columns = [row[0] for row in c.fetchall()]
        return columns

    def get_all_column_types(self, table_name, columns):
        column_types = {}
        sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = ANY(%s);
        """
        self.logger.debug(f"Executing SQL: {sql} with parameters: {table_name}, {columns}")  # Логуємо SQL-запит і параметри
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (table_name, columns))
            for column_name, data_type in cursor.fetchall():
                column_types[column_name] = data_type
        return column_types

    def get_foreign_keys(self, table):
        query = f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS referenced_table
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_name = '{table}' AND tc.constraint_type = 'FOREIGN KEY';
        """
        self.logger.debug(f"Executing SQL: {query}")  # Логуємо SQL-запит
        with self.conn.cursor() as c:
            c.execute(query)
            foreign_keys = {row[0]: row[1] for row in c.fetchall()}
        return foreign_keys

    def add_data(self, table, columns, values):
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(values))
        sql = f'INSERT INTO "public"."{table}" ({columns_str}) VALUES ({placeholders})'
        self.logger.debug(f"Executing SQL: {sql} with values: {values}")  # Логуємо SQL-запит і параметри
        with self.conn.cursor() as c:
            try:
                c.execute(sql, values)
                self.conn.commit()
                return "Дані успішно додано"
            except Exception as e:
                self.conn.rollback()
                return f"Помилка: {e}"

    def generate_doctor_data(self, count):
        self.execute_sql("TRUNCATE TABLE doctor RESTART IDENTITY CASCADE;")
        sql = f"""
        INSERT INTO doctor (doctor_id, s_f_p, specialization)
        SELECT 
            generate_series(1, {count}) AS doctor_id,
            chr(65 + trunc(random()*25)::int) || chr(65 + trunc(random()*25)::int) AS s_f_p,
            (ARRAY['Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Dermatology'])[trunc(random() * 5 + 1)::int] AS specialization
        """
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        self.execute_sql(sql)
        print("Doctors data generated.")

    def generate_patient_data(self, count):
        self.execute_sql("TRUNCATE TABLE patient RESTART IDENTITY CASCADE;")
        sql = f"""
        INSERT INTO patient (patient_id, s_f_p, gender, DOB)
        SELECT 
            generate_series(1, {count}) AS patient_id,
            chr(65 + trunc(random()*25)::int) || chr(65 + trunc(random()*25)::int) AS s_f_p,
            (ARRAY['Male', 'Female'])[trunc(random() * 2 + 1)::int] AS gender,
            date '1950-01-01' + trunc(random() * 18250)::int AS DOB  -- Random date between 1950 and 2000
        """
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        self.execute_sql(sql)
        print("Patients data generated.")

    def generate_medical_history_data(self, count):
        self.execute_sql("TRUNCATE TABLE medical_history RESTART IDENTITY CASCADE;")
        sql = f"""
        INSERT INTO medical_history (medical_history_id, diagnos_name, description, doctor_id, patient_id)
        SELECT 
            generate_series(1, {count}) AS medical_history_id,
            (ARRAY['Hypertension', 'Diabetes', 'Arthritis', 'Asthma', 'Heart Disease'])[trunc(random() * 5 + 1)::int] AS diagnos_name,
            (ARRAY[
                'Patient exhibits chronic symptoms.',
                'Requires immediate follow-up.',
                'Patient is responding well to treatment.',
                'Symptoms have shown mild improvement.'
            ])[trunc(random() * 4 + 1)::int] AS description,
            trunc(random() * {count} + 1)::int AS doctor_id,
            trunc(random() * {count} + 1)::int AS patient_id
        """
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        self.execute_sql(sql)
        print("Medical history data generated.")

    def generate_recept_data(self, count):
        self.execute_sql("TRUNCATE TABLE recept RESTART IDENTITY CASCADE;")
        sql = f"""
        INSERT INTO recept (recept_id, drug_name, medical_history_id)
        SELECT 
            generate_series(1, {count}) AS recept_id,
            (ARRAY['Paracetamol', 'Ibuprofen', 'Amoxicillin', 'Omeprazole', 'Atorvastatin'])[trunc(random() * 5 + 1)::int] AS drug_name,
            trunc(random() * {count} + 1)::int AS medical_history_id
        """
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        self.execute_sql(sql)
        print("Recepts data generated.")

    def generate_data(self, table, count):
        if table == 'doctor':
            self.generate_doctor_data(count)
        elif table == 'patient':
            self.generate_patient_data(count)
        elif table == 'medical_history':
            self.generate_medical_history_data(count)
        elif table == 'recept':
            self.generate_recept_data(count)
        else:
            print("Invalid table name. Please choose from 'doctor', 'patient', 'medical_history', 'recept'.")

    def read_data(self, table):
        try:
            sql = f'SELECT * FROM {table}'
            self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
            with self.conn.cursor() as c:
                c.execute(sql)
                return c.fetchall()
        except Exception as e:
            return f"Помилка: {e}"

    def delete_data(self, table, id):
        table_temp = table
        if table in ['doctor', 'patient', 'medical_history', 'recept']:
            table_temp = table + '_id'
        try:
            sql = f'DELETE FROM {table} WHERE {table_temp} = %s'
            self.logger.debug(f"Executing SQL: {sql} with parameter: {id}")  # Логуємо SQL-запит і параметри
            with self.conn.cursor() as c:
                c.execute(sql, (id,))
                self.conn.commit()
                return "Дані успішно видалено"
        except Exception as e:
            self.conn.rollback()
            return f"Помилка: {e}"

    def update_data(self, table, columns, id, new_values):
        columns_str = ', '.join([f"{col} = %s" for col in columns])
        table_temp = table
        if table in ['doctor', 'patient', 'medical_history', 'recept']:
            table_temp = table + '_id'
        try:
            sql = f'UPDATE {table} SET {columns_str} WHERE {table_temp} = %s'
            self.logger.debug(f"Executing SQL: {sql} with parameters: {new_values}, {id}")  # Логуємо SQL-запит і параметри
            with self.conn.cursor() as c:
                c.execute(sql, (*new_values, id))
                self.conn.commit()
                return "Дані успішно оновлено"
        except Exception as e:
            self.conn.rollback()
            return f"Помилка: {e}"

    def find_medical_history(self, doctor_id, patient_id):
        try:
            sql = """
                SELECT mh.medical_history_id, mh.diagnos_name, mh.description, d.s_f_p AS doctor_name, p.s_f_p AS patient_name
                FROM public.medical_history mh
                JOIN public.doctor d ON mh.doctor_id = d.doctor_id
                JOIN public.patient p ON mh.patient_id = p.patient_id
                WHERE mh.doctor_id = %s AND mh.patient_id = %s;
            """
            self.logger.debug(f"Executing SQL: {sql} with parameters: {doctor_id}, {patient_id}")  # Логуємо SQL-запит і параметри
            c = self.conn.cursor()
            c.execute(sql, (doctor_id, patient_id))
            columns = ["medical_history_id", "diagnos_name", "description", "doctor_s_f_p", "patient_s_f_p"]
            return c.fetchall(), columns, "Query executed successfully"
        except Exception as e:
            print(e)
            return [], []

    def find_recepts_by_medical_history(self, medical_history_id):
        try:
            sql = f"""
                SELECT r.recept_id, r.drug_name, mh.diagnos_name
                FROM recept r
                JOIN medical_history mh ON r.medical_history_id = mh.medical_history_id
                WHERE r.medical_history_id = %s;
            """
            self.logger.debug(f"Executing SQL: {sql} with parameter: {medical_history_id}")  # Логуємо SQL-запит і параметри
            with self.conn.cursor() as c:
                start_time = time.time()
                c.execute(sql, (medical_history_id,))
                elapsed_time = time.time() - start_time
                res_time_string = f"Час виконання запиту: {elapsed_time:.4f} секунд"
                columns = ["recept_id", "drug_name", "diagnos_name"]
                return c.fetchall(), columns, res_time_string
        except Exception as e:
            return f"Помилка: {e}"

    def close_connection(self):
        self.conn.close()