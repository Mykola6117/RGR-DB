import psycopg2
import time
import random
import string
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

# Entity classes representing database tables
class Doctor(Base):
    __tablename__ = 'doctor'
    doctor_id = Column(Integer, primary_key=True)
    s_f_p = Column(String(100))
    specialization = Column(String(40))
    medical_histories = relationship('MedicalHistory', back_populates='doctor')

class Patient(Base):
    __tablename__ = 'patient'
    patient_id = Column(Integer, primary_key=True)
    s_f_p = Column(String(100))
    gender = Column(String(1))
    dob = Column(Date)
    medical_histories = relationship('MedicalHistory', back_populates='patient')

class MedicalHistory(Base):
    __tablename__ = 'medical_history'
    medical_history_id = Column(Integer, primary_key=True)
    diagnos_name = Column(String(30))
    description = Column(String(1000))
    doctor_id = Column(Integer, ForeignKey('doctor.doctor_id'))
    patient_id = Column(Integer, ForeignKey('patient.patient_id'))
    doctor = relationship('Doctor', back_populates='medical_histories')
    patient = relationship('Patient', back_populates='medical_histories')
    recepts = relationship('Recept', back_populates='medical_history')

class Recept(Base):
    __tablename__ = 'recept'
    recept_id = Column(Integer, primary_key=True)
    drug_name = Column(String)
    medical_history_id = Column(Integer, ForeignKey('medical_history.medical_history_id'))
    medical_history = relationship('MedicalHistory', back_populates='recepts')


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
        
        # Database connection setup
        self.engine = create_engine('postgresql://postgres:Mamchyts@localhost:5432/postgres')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def execute_sql(self, sql):
        self.logger.debug(f"Executing SQL: {sql}")  # Логуємо SQL-запит
        with self.conn.cursor() as c:
            c.execute(sql)
            self.conn.commit()

    def get_all_tables(self):
        self.logger.debug("Getting all tables")
        with self.engine.connect() as connection:
            result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            return [row[0] for row in result.fetchall()]

    def get_all_columns(self, table_name):
        self.logger.debug(f"Getting columns for table: {table_name}")
        with self.engine.connect() as connection:
            result = connection.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                (table_name,)
            )
            return [row[0] for row in result.fetchall()]

    def get_all_column_types(self, table_name, columns):
        self.logger.debug(f"Getting column types for table: {table_name} with columns: {columns}")
        column_types = {}
        with self.engine.connect() as connection:
            result = connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = ANY(%s);
                """,
                (table_name, columns),
            )
            for column_name, data_type in result.fetchall():
                column_types[column_name] = data_type
        return column_types

    def get_foreign_keys(self, table):
        self.logger.debug(f"Getting foreign keys for table: {table}")
        with self.engine.connect() as connection:
            result = connection.execute(
                f"""
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
            )
            return {row[0]: row[1] for row in result.fetchall()}

    def add_data(self, table, columns, values):
        self.logger.debug(f"Adding data to table: {table} with columns: {columns} and values: {values}")
        session = self.Session()
        try:
            table_class = Base.metadata.tables[table]
            record = table_class(**dict(zip(columns, values)))
            session.add(record)
            session.commit()
            return "Data added successfully"
        except Exception as e:
            session.rollback()
            return f"Error: {e}"
        finally:
            session.close()

    def read_data(self, table):
        self.logger.debug(f"Reading data from table: {table}")
        session = self.Session()
        try:
            table_class = Base.metadata.tables[table]
            return session.query(table_class).all()
        except Exception as e:
            return f"Error: {e}"
        finally:
            session.close()

    def delete_data(self, table, id):
        self.logger.debug(f"Deleting data from table: {table} with id: {id}")
        session = self.Session()
        table_temp = table + '_id'
        try:
            table_class = Base.metadata.tables[table]
            session.query(table_class).filter(getattr(table_class, table_temp) == id).delete()
            session.commit()
            return "Data deleted successfully"
        except Exception as e:
            session.rollback()
            return f"Error: {e}"
        finally:
            session.close()

    def update_data(self, table, columns, id, new_values):
        self.logger.debug(f"Updating data in table: {table} with columns: {columns}, id: {id}, and new values: {new_values}")
        session = self.Session()
        table_temp = table + '_id'
        try:
            table_class = Base.metadata.tables[table]
            record = session.query(table_class).filter(getattr(table_class, table_temp) == id).first()
            for col, value in zip(columns, new_values):
                setattr(record, col, value)
            session.commit()
            return "Data updated successfully"
        except Exception as e:
            session.rollback()
            return f"Error: {e}"
        finally:
            session.close()

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