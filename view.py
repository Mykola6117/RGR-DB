import time

class View:
    def show_menu(self):
        while True:
            print("Menu:")
            print("1. Show table names")
            print("2. Show column names of a table")
            print("3. Add data to a table")
            print("4. Update data in a table")
            print("5. Delete data from a table")
            print("6. Generate data for a table")
            print("7. View data in a table")
            print("8. Find medical history by doctor and patient ID")
            print("9. Find prescription by medical history ID")
            print("10. Exit")

            choice = input("Please make a selection: ")

            if choice in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10'):
                return choice
            else:
                print("Please enter a valid option (1-10).")
                time.sleep(2)

    def show_message(self, message):
        print(message)
        time.sleep(2)

    def show_tables(self, tables):
        print("Table names:")
        for table in tables:
            print(table)
        time.sleep(2)

    def show_data(self, data: list, columns):
        for data_tuple in data:
            for i, el in enumerate(data_tuple):
                print(f'{columns[i]}: {el}', end=" ")
            print()
        time.sleep(2)

    def ask_table(self):
        table_name = input("Enter table name: ")
        return table_name

    def show_columns(self, columns):
        print("Column names:")
        for column in columns:
            print(column)
        time.sleep(2)

    def insert(self):
        while True:
            try:
                table = input("Enter table name: ")
                columns = input("Enter column names (space-separated): ").split()
                val = input("Enter corresponding values (space-separated): ").split()

                if len(columns) != len(val):
                    raise ValueError("The number of columns must be equal to the number of values.")

                return table, columns, val
            except ValueError as e:
                print(f"Error: {e}")

    def update(self):
        while True:
            try:
                table = input("Enter table name: ")
                columns = input("Enter column names (space-separated) to update: ").strip().split()
                id = int(input("Enter the ID of the row to update: "))
                new_values = input(f"Enter {len(columns)} new values: ").split()
                return table, columns, id, new_values
            except ValueError as e:
                print(f"Error: {e}")

    def delete(self):
        while True:
            try:
                table = input("Enter table name: ")
                id = int(input("Enter the ID of the row to delete: "))
                return table, id
            except ValueError as e:
                print(f"Error: {e}")

    def generate_data_input(self):
        while True:
            try:
                table_name = input("Enter table name: ")
                rows_count = int(input("Enter the number of rows to generate: "))
                return table_name, rows_count
            except ValueError as e:
                print(f"Error: {e}")

    def get_medical_history_input(self):
        while True:
            try:
                doctor_id = int(input("Enter doctor ID: "))
                patient_id = int(input("Enter patient ID: "))
                return doctor_id, patient_id
            except ValueError as e:
                print(f"Error: {e}")

    def get_recept_input(self):
        while True:
            try:
                medical_history_id = int(input("Enter medical history ID: "))
                return medical_history_id
            except ValueError as e:
                print(f"Error: {e}")