import csv
import re
import pandas as pd
import tkinter as tk
from database import DatabaseUploader
from preprocessing_datas import PreprocDatasRegions
from pandastable import Table, TableModel
from prs_processing import PrsProcessor
import io
import numpy as np
from tkinter import messagebox


class FileProcessor:
    def __init__(self):
        self.csv_file = None
        self.results = []
        self.errors = []

    def process_files(self, files):
        self.results = []
        self.errors = []
        self.result_dict = {'ship': [], 'reis': [], 'nst_g': [], 'date': [], 'shir_dsec': [], 'dolg_dsec': [], 'glub': [], 
                        'press': [], 'st_time': [], 't_air': [], 'v_wind': [], 'f_wind': [], 'v_wave': [], 'h_wave': [],
                        'visibil': [], 'cloud': [], 'clo_nyr': [], 'for_clo': [], 'weather': [], 'operator': [] }          

        for file in files:
            try:
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith('==='):
                            continue
                        if line.startswith('  Vessel:'):                       
                            self.ship, self.reis = re.findall(r'\w+|\w+ \w+', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'\w*', line[line.find(':', 32)+1:line.find('|', 48)])
                            if self.ship[0] != '' and len(self.ship) == 1:
                                self.result_dict["ship"].append(self.ship[0])
                            elif self.ship[0] != '' and self.ship[1] != '' and len(self.ship) == 2:
                                name_ship = [i for i in self.ship[0:2]]
                                self.ship = ' '.join(name_ship)
                                self.result_dict["ship"].append(self.ship)
                            self.result_dict['reis'].append(self.reis[1]) 
# 2. Номер станции и оператор
                        elif line.startswith('   Sta.#:'):
                            self.station, self.operator = re.findall(r'\w*', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'\w*', line[line.find(':', 32)+1:line.find('|', 48)])
                            self.result_dict['nst_g'].append(self.station[1])
                            self.result_dict['operator'].append(self.operator[1])
# скип
                        elif line.startswith(' Inst. #:'):
                            pass
 # 4. Дата
                        elif line.startswith('CAL Extn:'):
                            self.date = re.findall(r'\d{1,2}-[A-Z]{3}-\d{4}', line[line.find(':', 32)+1:line.find('|', 48)])
                            self.result_dict['date'].append(self.date[0]) 
 # 5. Время и давление
                        elif line.startswith(' St.Time:'):
                            self.st_time, self.press = re.findall(r'\d{1,2}:\d{2}:', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'\d*\.?\d+', line[line.find(':', 32)+1:line.find('|', 48)])

                            if self.press == []:
                                self.press.extend([""])
                            if self.press[0] == "":
                                self.press[0] = None
                                self.result_dict['press'].append(self.press[0])
                            else:
                                self.result_dict['press'].append(float(self.press[0]))

                            if self.st_time == []:
                                self.st_time.extend([""])
                            if self.st_time[0] == "":
                                self.st_time[0] = None
                                self.result_dict['st_time'].append(self.st_time[0])
                            else:
                                self.result_dict['st_time'].append(self.st_time[0])

# 6. Широта и температура воздуха
                        elif line.startswith(' St.Lat.:'):
                            self.st_lat, self.t_air = re.findall(r'-?\d{1,3}(?::\d{2}){1,2}(?:\.\d{1,3})?', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'-?\d*\.?\d+', line[line.find(':', 32)+1:line.find('|', 48)])

                            if self.st_lat == []:
                                self.st_lat.extend([""])
                            if self.st_lat[0] == "":
                                self.st_lat[0] = None
                                self.result_dict['shir_dsec'].append(self.st_lat[0])
                            else:
                                self.result_dict['shir_dsec'].append(self.st_lat[0])   

                            if self.t_air == []:
                                self.t_air.extend([""])
                            if self.t_air[0] == "":
                                self.t_air[0] = None
                                self.result_dict['t_air'].append(self.t_air[0])
                            else:
                                self.result_dict['t_air'].append(float(self.t_air[0])) 


# 7. Долгота и ветер (градус и скорость)
                        elif line.startswith(' St.Long:'):
                            self.st_lon, self.wind = re.findall(r'-?\d{1,3}(?::\d{2}){1,2}(?:\.\d{1,3})?', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'(-?\d*\.?\d+)*/(-?\d*\.?\d+)*', line[line.find(':', 32)+1:line.find('|', 48)])
                            self.wind = list(self.wind[0])

                            if self.wind[0] == "":
                                self.wind[0] = None
                                self.result_dict['v_wind'].append(self.wind[0])
                            else:
                                self.result_dict['v_wind'].append(int(self.wind[0]))

                            if self.wind[1] == "":
                                self.wind[1] = None
                                self.result_dict['f_wind'].append(self.wind[1])
                            else:
                                self.result_dict['f_wind'].append(float(self.wind[1]))

                            if self.st_lon == []:
                                self.st_lon.extend([""])
                            if self.st_lon[0] == "":
                                self.st_lon[0] = None
                                self.result_dict['dolg_dsec'].append(self.st_lon[0])
                            else:
                                self.result_dict['dolg_dsec'].append(self.st_lon[0])

# 8. Глубина и параметры волны
                        elif line.startswith('   Depth:'):
                            self.depth, self.wave = re.findall(r'\w*', line[line.find(':', 8)+1:line.find('|', 22)]), re.findall(r'(-?\d*\.?\d+)*/(-?\d*\.?\d+)*', line[line.find(':', 32)+1:line.find('|', 48)])
                            
                            if self.depth == []:
                                self.depth.extend([""])
                            if self.depth[0] == "":
                                self.depth[0] = None
                                self.result_dict['glub'].append(self.depth[1])
                            else:
                                self.result_dict['glub'].append(self.depth[1])

                            self.wave = list(self.wave[0])

                            if self.wave[0] == "":
                                self.wave[0] = None
                                self.result_dict['v_wave'].append(self.wave[0])
                            else:
                                self.result_dict['v_wave'].append(int(self.wave[0]))

                            if self.wave[1] == "":
                                self.wave[1] = None
                                self.result_dict['h_wave'].append(self.wave[1])
                            else:
                                self.result_dict['h_wave'].append(float(self.wave[1]))
                                                                               
# 9. Облачность
                        elif line.startswith('AverOver:'):
                             
                            self.cloud = re.findall(r'(-?\d*\.?\d+)*/(-?\d*\.?\d+)*/?([a-zA-Z]+)*\s?/?([a-zA-Z]+)*', line[line.find(':', 32)+1:line.find('|', 48)])

                            if self.cloud == []:
                                self.cloud.extend(["//"])
                                self.cloud = self.cloud[0].split("/")
                            else:
                                self.cloud = '/'.join(self.cloud[0])
                                self.cloud  = self.cloud.split("/")
                            
                            if self.cloud[0] == '':
                                self.cloud[0] = None
                                self.result_dict['cloud'].append(self.cloud[0])
                            else:
                                self.result_dict['cloud'].append(self.cloud[0])

                            if self.cloud[1] == '':
                                self.cloud[1] = None   
                                self.result_dict['clo_nyr'].append(self.cloud[1])
                            else:
                                self.result_dict['clo_nyr'].append(self.cloud[1])   

                            if self.cloud[2] == '':
                                self.result_dict['for_clo'].append(None)                                
                            else:                       
                                self.for_clo = [ i for i in self.cloud[2:]]
                                self.for_clo = " ".join(self.for_clo)
                                self.result_dict['for_clo'].append(self.for_clo)

# 10. Видимость
                        elif line.startswith('DPTH* Min:             |Visibility:') or line.startswith('TEMP* Min:             |Visibility:'):
                            self.visibil = re.findall(r'\w*', line[line.find(':', 32)+1:line.find('|', 48)])
                            
                            if self.visibil[1] == "":
                                self.visibil[1] = None
                                self.result_dict['visibil'].append(self.visibil[1])
                            else:
                                self.result_dict['visibil'].append(float(self.visibil[1]))

# 11. Погода
                        elif line.startswith('SALT* Min:             |   Weather:') or line.startswith('TEMP* Min:             |   Weather:'):
                            self.weather = re.findall(r'\w*', line[line.find(':', 32)+1:line.find('|', 48)])
                            if self.weather[1] == "":
                                self.weather[1] = None
                                self.result_dict['weather'].append(self.weather[1])
                            else:
                                self.result_dict['weather'].append(self.weather[1])
                                                                                             
                    self.df = pd.DataFrame(self.result_dict)

                    with open(file, 'r', encoding="utf-8") as all_text_file:
                        lines = all_text_file.readlines()
                    data_start_index = None
                    for i, line in enumerate(lines):
                        if '-------------------------------------------------' in line:
                            data_start_index = i + 1
                            data_start_index_no_names = i + 2
                            break
                    if data_start_index is None:
                        continue
                    data_lines = lines[data_start_index:]
                    data_lines_no_names = lines[data_start_index_no_names:]

                    n = data_lines[0].strip().split("*")
                    temp_df = pd.read_csv(io.StringIO(''.join(data_lines_no_names)), delim_whitespace=True,names=n)                
                    temp_df = temp_df.rename(columns=lambda x: x.strip())
                    temp_df = temp_df.applymap(lambda x: x.replace(",", "") if isinstance(x, str) else x)
                    temp_df_gor = temp_df['DPTH'].astype(float)

                    prev_val_gor = temp_df_gor.iloc[1]
                    is_interpolated_arr = []
                    for index, row in temp_df.head(9).iterrows():
                        if index > 1:
                            current_val_gor = float(row['DPTH'])
                            difference = current_val_gor - prev_val_gor
                            if difference <= 2:
                                is_interpolated_arr.append(False)
                            elif difference >= 10:
                                is_interpolated_arr.append(True)
                            prev_val_gor = current_val_gor
                    is_interpolated = is_interpolated_arr[0]
                    

            except IOError as e:
                 print(f"Error reading file: {file}")
                 self.file_report_text.insert(tk.END, f"ТУТ: {e}\n")

# ---------------------------------------------------------------------------------------------------- 
# Тут мы проходимся по полученному выше массиву с целью сделать словарь (структуру с полями, как в БД), которая будет использоваться как таблица далее.
        self.cursor = DatabaseUploader.connect(self, db, user, pass, host)
        try:
            query_ship = f"SELECT radio_sign FROM ships_reestr WHERE ves_lat LIKE '%{self.df['ship'][0].upper()}%';"
            # self.cursor[0].execute(query_ship, ('%'+self.df['ship'][0].upper()+'%',))
            self.cursor[0].execute(query_ship)
            self.ship_pos = self.cursor[0].fetchone()[0]
            print(self.ship_pos)
            # Заменяем название судна на радио-сигнал в столбце 'ship'
            self.df.loc[self.df['ship'] == self.df['ship'][0], 'ship'] = self.ship_pos
        except Exception as e:
            messagebox.showinfo("Ошибка", f"Возникла ошибка: {e}")

        # Преобразуем столбец 'date' в тип datetime и изменяем его формат на 'YYYY-MM-DD'
        try:
            self.file_report_text.delete("1.0", tk.END)
            self.df['date'] = pd.to_datetime(self.df['date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')
        except ValueError:
            self.file_report_text.delete("1.0", tk.END)
            self.file_report_text.insert(tk.END, f"Дата имеет некорректное значение. Проверьте, пожалуйста. Ошибка в файле № {self.df['nst_g'][0]}.\n")
            self.df['date'] = '0'
        
        # Преобразуем координаты широты и долготы из градусов, минут и секунд в десятичные градусы
        self.df['shir_dsec'] = self.df['shir_dsec'].str.replace(':', '').str.replace('.', '').astype(float) / 10
        self.df['dolg_dsec'] = self.df['dolg_dsec'].str.replace(':', '').str.replace('.', '').astype(float) / 10
        self.df['shir_dgr'] = self.df['shir_dsec'].apply(lambda x: int((x/100) / 100) + ((x/100) / 100 - int((x/100) / 100)) / 6 * 10)
        self.df['dolg_dgr'] = self.df['dolg_dsec'].apply(lambda x: int((x/100) / 100) + ((x/100) / 100 - int((x/100) / 100)) / 6 * 10)

        # Преобразуем пустые строки в значени null
        # self.df['cloud'] = self.df['cloud'].replace('', None)
        # self.df['clo_nyr'] = self.df['clo_nyr'].replace('', None)
        # self.df['for_clo'] = self.df['for_clo'].replace('', None)

        # Объединяем столбцы 'date' и 'st_time' в столбец 'datatime'
        self.df['datatime'] = self.df.apply(lambda row: row['date'] + ' ' + row['st_time'] + '00', axis=1)
# ----------------------------------------------------------------------------------------------------
        self.cursor_reg = DatabaseUploader.connect(self, db, user, pass, host)
        self.cursor_reg[0].execute('SELECT zona, shir, dolg FROM full_408;')
        self.qur_eco = self.cursor_reg[0].fetchall()
        self.df_polygons = pd.DataFrame(self.qur_eco, columns=['rnzon', 'lat', 'lon'])
        self.polygons_eco = self.df_polygons.groupby('rnzon').apply(lambda x: x[['lat', 'lon']].astype(float).values.tolist()).to_dict()
        self.points = list(map(tuple, self.df[['shir_dgr', 'dolg_dgr']].values.tolist()))
        self.result_eco = PreprocDatasRegions.assign_polygon_id(self, self.points, self.polygons_eco) 
        self.list_ecos = []
        for point in self.points:
            for key, value in self.result_eco.items():
                if (tuple(point) in value):
                    self.list_ecos.append(key)
        self.df["rnzon"] = self.list_ecos
# ----------------------------------------------------------------------------------------------------
        self.cursor_reg[0].execute('SELECT region,shir,dolg from full_520;')
        self.qur_ikes = self.cursor_reg[0].fetchall()
        self.df_polygons_ikes = pd.DataFrame(self.qur_ikes, columns=['ikes', 'lat', 'lon'])
        self.polygons_ikes = self.df_polygons_ikes.groupby('ikes').apply(lambda x: x[['lat', 'lon']].astype(float).values.tolist()).to_dict()
        self.result_ikes = PreprocDatasRegions.assign_polygon_id(self, self.points, self.polygons_ikes)
        self.list_ikes = []
        for point in self.points:
            for key, value in self.result_ikes.items():
                if (tuple(point) in value):
                    self.list_ikes.append(key)
        self.df["rnikes"] = self.list_ikes
# ----------------------------------------------------------------------------------------------------
        self.cursor_reg[0].execute('SELECT rnloc, startlat, endlat, startlon, endlon FROM regloc_full;')
        self.qur_regloc = self.cursor_reg[0].fetchall()
        self.df_rnloc = pd.DataFrame(self.qur_regloc, columns=['rnloc', 'startlat', 'endlat', 'startlon', 'endlon'])
        self.local_areas = self.df_rnloc.groupby('rnloc').apply(lambda x: [[float(i) for i in x['startlat']], 
                                                                           [float(i) for i in x['endlat']], 
                                                                           [float(i) for i in x['startlon']], 
                                                                           [float(i) for i in x['endlon']]]).to_dict()
        for name, area in self.local_areas.items(): #эта штука определяет центр района
            x = sum(area[0]) / len(area[0]) + sum(area[1]) / len(area[1])
            y = sum(area[2]) / len(area[2]) + sum(area[3]) / len(area[3])
            area.append((x/2, y/2))

        self.result_regloc = PreprocDatasRegions.assign_regloc_id(self, self.points, self.local_areas)
        self.list_regions = []
        for point in self.points:
            for key, value in self.result_regloc.items():
                if (tuple(point) in value):
                    self.list_regions.append(key)
        # print([int(i) for i in self.list_regions])
        # print(self.df[['shir_dgr', 'dolg_dgr']])
        self.df['rnloc'] = self.list_regions
        self.df['rnloc'] = self.df['rnloc'].fillna(0)
        self.df['rnloc'] = self.df['rnloc'].astype(int)

        # self.df['rnloc'] = self.list_regions

        self.download_dict = {'ship': list(self.df['ship']), 'reis': list(self.df['reis']), 'nst_g': list(self.df['nst_g']),
                        'nst_p': None, 'nst_b': None, 'nst_l': None, 'nst_yr': None, 'trl': None, 'datatime': list(self.df['datatime']),
                        'rnloc': list(self.df['rnloc']), 'rnzon': list(self.df['rnzon']), 'rnikes': list(self.df['rnikes']), 
                        'shir_dgr': list(self.df['shir_dgr']), 'dolg_dgr': list(self.df['dolg_dgr']),
                        'shir_dsec': list(self.df['shir_dsec']), 'dolg_dsec': list(self.df['dolg_dsec']), 'glub': list(self.df['glub']),
                        'press': list(self.df['press']), 't_air': list(self.df['t_air']), 'v_wind': list(self.df['v_wind']), 'f_wind': list(self.df['f_wind']),
                        'v_wave': list(self.df['v_wave']), 'h_wave': list(self.df['h_wave']), 'visibil': list(self.df['visibil']), 'cloud': list(self.df['cloud']),
                        'clo_nyr': list(self.df['clo_nyr']), 'for_clo': list(self.df['for_clo']), 'tr_water': None, 'rzr1': None, 'rzr2': None, 'weather': list(self.df['weather']),
                        'operator': list(self.df['operator']), 'interpolated': is_interpolated}        
        self.new_df = pd.DataFrame(self.download_dict)
        self.new_df[['press', 't_air', 'v_wind', 'f_wind', 'v_wave', 'h_wave', 
                     'visibil', 'cloud', 'clo_nyr', 'weather']] = self.new_df[['press', 't_air', 'v_wind', 'f_wind', 'v_wave', 'h_wave', 
                                                                               'visibil', 'cloud', 'clo_nyr', 'weather']].astype(float)        
        pd.set_option('display.max_rows', None)

        select_query = f"SELECT MAX(stkl) FROM station_t;"

        # Тут мы отображаем в виде таблички все полученные и обработанные данные, которые будем грузить.
        # self.file_contents_text.insert(tk.END, f"{self.new_df}\n")
        # return self.new_df
        # self.table.redraw()

        self.table.model.df = self.new_df
        self.table.redraw()

        



# ----------------------------------------------------------------------------------------------------       
