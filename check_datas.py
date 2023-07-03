import tkinter as tk
from math import radians, cos, sqrt, sin, atan2, pi
from datetime import datetime

class CheckDatas:
# ----------------------------------------------------------------------------------------------------
    def call_check_range(self, notebook):
        if notebook == "Station":
            self.file_report_text.delete("1.0", tk.END)
            
            self.col_ranges_station = { 'press': (800,1100), 't_air': (-50,50), 'v_wind': (0,359), 'f_wind': (0,50), 
                                        'v_wave': (0,359), 'h_wave': (0, 9.9), 'visibil': (0,50), 'cloud': (0,10), 
                                        'clo_nyr': (0,10), 'weather': (0,99)}
            self.result_report, self.message_report = CheckDatas.check_range(self, self.new_df, self.col_ranges_station)
            self.result_velocity, self.message_velocity = CheckDatas.check_velocity(self, self.new_df)
            self.result_params, self.message_params = CheckDatas.check_params(self, self.new_df)

            if self.result_params:
                self.file_report_text.insert(tk.END, f"{self.message_params}")
            else:
                for error in self.message_params:
                    self.file_report_text.insert(tk.END, error)
            
            if self.result_velocity:
                self.file_report_text.insert(tk.END, f"{self.message_velocity}")
            else:
                for error in self.message_velocity:
                    self.file_report_text.insert(tk.END, error)

            if self.result_report:
                self.file_report_text.insert(tk.END, f"{self.message_report}")
            else:
                for error in self.message_report:
                    # print(error)
                    self.file_report_text.insert(tk.END, error)

        elif notebook == "Horizon":
            self.file_report_text_horizon.delete("1.0", tk.END)
            self.col_ranges_horizon = {'t': (-2.00,30.00), 'sl': (10.000,36.200)}
            self.result_report, self.message_report = CheckDatas.check_range(self, self.horizon_table, self.col_ranges_horizon)

            if self.result_report:
                self.file_report_text_horizon.insert(tk.END, f"{self.message_report}")
            else:
                for error in self.message_report:
                    self.file_report_text_horizon.insert(tk.END, error)

        elif notebook == "Chem elem":
            self.file_report_text_chem_elem.delete("1.0", tk.END)
            self.col_ranges_chem_elem = {'o2': (2.00, 15.00), 'po4': (0.00, 2.50), 'sio3': (0.0, 80.0),
                                         'nh4': (0.00, 20.00), 'no2': (0.00, 9.99), 'no3': (0.00, 99.99),
                                         'ph': (0.00, 14.00), 'bpk': (0.00, 11.00), 'no': (0.0, 999.9), 
                                         'po': (0.0, 99.90)}   
            self.result_report, self.message_report = CheckDatas.check_range(self, self.chem_elem_table, self.col_ranges_chem_elem)
            
            if self.result_report:
                self.file_report_text_chem_elem.insert(tk.END, f"{self.message_report}")
            else:
                for error in self.message_report:
                    self.file_report_text_chem_elem.insert(tk.END, error)
          
    def check_velocity(self, df):
        errors_v = []

        prev_shir = df['shir_dgr'][0]
        prev_dolg = df['dolg_dgr'][0]
        prev_nst_g = df['nst_g'][0]
        prev_time = datetime.strptime(df['datatime'][0], '%Y-%m-%d %H:%M:%S')
        
        for index, row in df.iterrows():
            if index > 0:
                current_shir = row['shir_dgr']
                current_dolg = row['dolg_dgr']
                current_nst_g = row['nst_g']
                current_time = datetime.strptime(row['datatime'], '%Y-%m-%d %H:%M:%S')

                delta = (current_time - prev_time)
                delta_time_distance = delta.total_seconds() / 3600 # получаем количество дней и преобразуем его в целое число

                s = sqrt(((prev_shir - current_shir) * 60.0)**2 + (((prev_dolg - current_dolg) * 60.0) * (cos(((prev_shir + current_shir) * 0.5) * pi / 180.0)))**2)
                v = s/delta_time_distance

                if v > 15.00:
                    errors_v.append(f"ПРЕДУПРЕЖДЕНИЕ!!! Скорость больше 15 узлов ({v:.f4}), первая станция № {prev_nst_g} и вторая станция № {current_nst_g}. Расстояние от станции № {prev_nst_g} до станции № {current_nst_g}: {s:.f4} (миль). Время в пути {delta_time_distance} (часов.минут).\n")
                elif v > 17.00:
                    errors_v.append(f"ОШИБКА!!! Скорость больше 17 узлов ({v:.f4}), первая станция № {prev_nst_g} и вторая станция № {current_nst_g}. Расстояние от станции № {prev_nst_g} до станции № {current_nst_g}: {s:.f4} (миль). Время в пути {delta_time_distance} (часов.минут).\n")
                prev_shir = current_shir
                prev_dolg = current_dolg
                prev_time = current_time

        if errors_v:
            return False, errors_v
        else:
            return True, "Со скоростью все в порядке. Ошибок нет.\n"
        
    def check_params(self, df):
        errors_params = []

        prev_ship = df['ship'][0]
        prev_reis = df['reis'][0]
        prev_nst_g = df['nst_g'][0]
        prev_date = datetime.strptime(df['datatime'][0], '%Y-%m-%d %H:%M:%S')

        for index, row in df.iterrows():
            if index > 0:
                current_ship = row['ship']
                current_reis = row['reis']
                current_nst_g = row['nst_g']
                current_date = datetime.strptime(row['datatime'], '%Y-%m-%d %H:%M:%S')

                if prev_ship != current_ship:
                    errors_params.append(f"ОШИБКА!!! Обнаружен второй позывной для одного судна. Предыдущая станция = {prev_nst_g}, следующая станция = {current_nst_g}.\n")
                if prev_reis != current_reis:
                    errors_params.append(f"ОШИБКА!!! Номер рейса не совпадает для одного судна. Предыдущая станция = {prev_nst_g}, следующая станция = {current_nst_g}.\n")
                if int(prev_nst_g) > int(current_nst_g):
                    errors_params.append(f"ОШИБКА!!! Нарушен порядок гидрологических станций. Предыдущая станция = {prev_nst_g}, следующая станция = {current_nst_g}.\n")
                if int(prev_nst_g) == int(current_nst_g):
                    errors_params.append(f"ОШИБКА!!! Обнаружен дубликат гидрологической станции. Предыдущая станция = {prev_nst_g}, следующая станция = {current_nst_g}.\n")
                if prev_date > current_date:
                    errors_params.append(f"ОШИБКА!!! Проверьте дату, станция № {current_nst_g}.\n")

                prev_ship = current_ship
                prev_reis = current_reis
                prev_nst_g = current_nst_g
                prev_date = current_date
        if errors_params:
            return False, errors_params
        else:
            return True, "Параметры (позывной, рейс, даты, номера станций) проверены. Ошибок нет.\n"               

# ----------------------------------------------------------------------------------------------------
    def check_range(self, df, col_ranges):
        """
        Проверяет, что все значения в указанных столбцах DataFrame находятся в заданном диапазоне.
        Возвращает True, если все значения входят в диапазон, иначе возвращает False и список ошибок.
        """
        self.errors = []  
        for col, (min_value, max_value) in col_ranges.items():
            # print(col, (min_value, max_value))
            self.mask_notna = df[col].notna()
            self.mask_range = (df[col][self.mask_notna] >= min_value) & (df[col][self.mask_notna] <= max_value)
            if not self.mask_range.all():
                # Найдено значение, не входящее в диапазон
                self.mask_invalid = self.mask_notna & ~self.mask_range
                # self.mask = ~((df[col] >= min_value) & (df[col] <= max_value))
                self.invalid_values = df.loc[self.mask_invalid, col]
                for i, val in self.invalid_values.items():
                    self.errors.append(f"ОШИБКА: Значение {val} в столбце {col} на позиции {i+1} не входит в диапазон [{min_value}, {max_value}]. Рейс {df.iloc[i]['reis']}, станция {df.iloc[i]['nst_g']}\n")
        if self.errors:
            return False, self.errors
        else:
            return True, "Ошибок нет, все хорошо, можно грузить!\n"
        
# ----------------------------------------------------------------------------------------------------
    def check_empty_values(self, df):
        """
        Проверяет, что все значения в указанных столбцах DataFrame заполнены значениями.
        """
        for column in df:
            if df[column].isnull().values.any():
                print(f"{column} в позиции {df.columns.get_loc(column)} не заполнен значениями!")
