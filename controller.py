from model import Model
from view import View


class Controller:
    def __init__(self):
        self.view = View()
        self.model = Model()

    def run(self):
        while True:
            choice = self.view.show_menu()
            if choice == '1':
                self.view_tables()
            elif choice == '2':
                self.view_columns()
            elif choice == '3':
                self.add_data()
            elif choice == '4':
                self.update_data()
            elif choice == '5':
                self.delete_data()
            elif choice == '6':
                self.generate_data()
            elif choice == '7':
                self.read_data()
            elif choice == '8':
                self.find_medical_history()
            elif choice == '9':
                self.find_recepts_by_medical_history()
            elif choice == '10':
                break

    def view_tables(self):
        tables = self.model.get_all_tables()
        self.view.show_tables(tables)

    def view_columns(self):
        table = self.view.ask_table()
        columns = self.model.get_all_columns(table)
        self.view.show_columns(columns)

    def add_data(self):
        table, columns, val = self.view.insert()
        message = self.model.add_data(table, columns, val)
        self.view.show_message(message)

    def read_data(self):
        table = self.view.ask_table()
        columns = self.model.get_all_columns(table)
        data = self.model.read_data(table)
        self.view.show_data(data, columns)

    def delete_data(self):
        table, id = self.view.delete()
        message = self.model.delete_data(table, id)
        self.view.show_message(message)

    def update_data(self):
        table, columns, id, new_values = self.view.update()
        message = self.model.update_data(table, columns, id, new_values)
        self.view.show_message(message)

    def generate_data(self):
        table, rows_count = self.view.generate_data_input()
        message = self.model.generate_data(table, rows_count)
        self.view.show_message(message)

    def find_medical_history(self):
        doctor_id, patient_id = self.view.get_medical_history_input()
        data, columns, res_time = self.model.find_medical_history(doctor_id, patient_id)
        if len(data) != 0:
            self.view.show_data(data, columns)
            self.view.show_message(res_time)
        else:
            self.view.show_message("Нічого не знайдено за цією інформацією.")

    def find_recepts_by_medical_history(self):
        medical_history_id = self.view.get_recept_input()
        data, columns, res_time = self.model.find_recepts_by_medical_history(medical_history_id)
        if len(data) != 0:
            self.view.show_data(data, columns)
            self.view.show_message(res_time)
        else:
            self.view.show_message("Нічого не знайдено за цим ID рецепта.")

