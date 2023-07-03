"""
файл, который содержит функции обработки prs-файлов для таблиц Horizon и Chem_elem
"""

import pandas as pd
from database import DatabaseUploader
import io
import configparser

class PrsProcessor:

    def prs_read_and_prepocessing(self, files, notebook, btn_text):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.db = DatabaseUploader.connect(self,
                                           config['postgresql']['database'],
                                           config['postgresql']['user'],
                                           config['postgresql']['password'],
                                           config['postgresql']['host'])

        self.conn = self.db[1]
        self.cursor = self.db[0]

        self.columns = ['DPTH', 'TEMP', 'SALT', 'SGTH', 'CREM', 'PHOS', 'OXGN', 'OXGP', 
                        'TURB', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT', 'NTOT', 'BPKP', 'NOBS']
        self.df = pd.DataFrame(columns=self.columns)
        self.res_df_chemy = {}

        for filename in files:
            try:
                with open(filename, "r", encoding="utf-8") as text_file:
                    self.lines = text_file.readlines()

                self.ship_reis = self.lines[0].split("|")
                self.nst_g = self.lines[1].split("|")
                
                self.data_start_index = None
                for i, line in enumerate(self.lines):
                    if '-------------------------------------------------' in line:
                        self.data_start_index = i + 1
                        self.data_start_index_no_names = i + 2
                        break
                if self.data_start_index is None:
                    continue
                self.data_lines = self.lines[self.data_start_index:]
                self.data_lines_no_names = self.lines[self.data_start_index_no_names:]

                self.n = self.data_lines[0].strip().split("*")
                self.temp_df = pd.read_csv(io.StringIO(''.join(self.data_lines_no_names)), delim_whitespace=True,names=self.n)                
                self.temp_df = self.temp_df.rename(columns=lambda x: x.strip())

                self.temp_df["pr"] = 0
                self.temp_df.loc[self.temp_df.index[-1], "pr"] = 9
                self.temp_df["ship"] = self.ship_reis[0].split(":")[1].strip()
                self.temp_df['reis'] = self.ship_reis[1].split(":")[1].strip()
                self.temp_df['nst_g'] = self.nst_g[0].split(":")[1].strip()

                self.cursor.execute('SELECT radio_sign FROM ships_reestr WHERE ves_lat like %s', ('%'+self.temp_df['ship'][0].upper()+'%',))
                self.ship_pos = self.cursor.fetchone()[0]
                self.temp_df.loc[self.temp_df['ship'] == self.temp_df['ship'][0], 'ship'] = self.ship_pos

                self.temp_df['ship'] = self.temp_df['ship'].str.strip()
                self.df = pd.concat([self.df, self.temp_df], ignore_index=True)

                self.res_df_chemy = self.df[['ship', 'reis', 'nst_g', 'DPTH', 'TEMP', 'SALT', 'SGTH', 'CREM', 'PHOS', 'OXGN', 'OXGP', 'TURB', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT', 'NTOT', 'BPKP', 'pr']]

            except IOError:
                print(f"Error reading file: {filename}")
        
        self.horizon_table = PrsProcessor.preprocessing_df(self.res_df_chemy)[0]
        self.chem_elem_table = PrsProcessor.preprocessing_df(self.res_df_chemy)[1]

        if notebook == "Horizon":
            if btn_text == "Обработать PRS":
                self.table_horizon_prs.model.df = self.horizon_table
                self.table_horizon_prs.redraw()
                self.prs_table = self.horizon_table

            elif btn_text == "Обработать Mini PRS":
                self.table_horizon_prs_Miniprs.model.df = self.horizon_table
                self.table_horizon_prs_Miniprs.redraw()
                self.mini_table = self.horizon_table

            elif btn_text == "Объединить таблицы PRS и Mini PRS":
                self.merged_df = self.prs_table.merge(self.mini_table, on=['ship', 'reis', 'nst_g', 'gor'], how='left', suffixes=('_x', '_y'))

                # используем метод update для замены значений в столбце 'pr_chemi' первого датафрейма значениями из второго датафрейма
                self.merged_df['pr_chemi_x'].update(self.merged_df['pr_chemi_y'])

                # удаляем столбцы, которые больше не нужны
                self.merged_df = self.merged_df.drop(labels=['pr_chemi_y', 't_y', 'sl_y', 'pr_y'], axis=1)

                # переименовываем столбцы
                self.merged_df = self.merged_df.rename(columns={'pr_chemi_x': 'pr_chemi', 't_x': 't', 'sl_x': 'sl', 'pr_x': 'pr'})                
                
                self.table_horizon_prs_all.model.df = self.merged_df
                self.table_horizon_prs_all.redraw()

                self.table_uploading_horizon.model.df = self.merged_df
                self.table_uploading_horizon.redraw()            

        elif notebook == "Chem elem":
            self.table_chem_elem.model.df = self.chem_elem_table
            self.table_chem_elem.redraw()

            self.table_uploading_chem_elem.model.df = self.chem_elem_table
            self.table_uploading_chem_elem.redraw()

    def preprocessing_df(df):
        df = df.applymap(lambda x: x.replace(',', '') if isinstance(x, str) else x)
        df[['TEMP', 'SALT', 'OXGN', 'OXGP', 'BPKP', 'CREM', 
            'PHOS', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT']] = df[['TEMP', 'SALT', 'OXGN', 'OXGP', 'BPKP', 'CREM', 
                                                                    'PHOS', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT']].applymap(lambda x: x.replace('999.00', 'None').replace('999.0', 'None') if isinstance(x, str) else x)
        df = df.replace('None', None)
        df[['TEMP', 'SALT', 'OXGN', 'OXGP', 'BPKP', 'CREM', 
            'PHOS', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT']] = df[['TEMP', 'SALT', 'OXGN', 'OXGP', 'BPKP', 'CREM', 
                                                                'PHOS', 'NTRI', 'NTRA', 'NHVL', 'PHVL', 'PTOT']].astype(float)

        df.rename(columns={'TEMP': 't', 'SALT': 'sl', 'DPTH': 'gor', 'OXGN': 'o2', 'BPKP': 'bpk',
                            'CREM': 'sio3', 'PHOS': 'po4', 'NTRI': 'no2', 'NTRA': 'no3', 'NHVL': 'nh4',
                            'PTOT': 'po', 'PHVL': 'ph', 'TURB': 'turb', 'NTOT': 'no' }, inplace = True)
        
        df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
            'ph', 'po', 'bpk', 'turb', 'no']] = df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
                                                        'ph', 'po', 'bpk', 'turb', 'no']].fillna(0)

        df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
            'ph', 'po', 'bpk', 'turb', 'no']] = df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
                                                        'ph', 'po', 'bpk', 'turb', 'no']].fillna(0)
    
        df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
            'ph', 'po', 'bpk', 'turb', 'no']] = df[['sio3', 'po4', 'o2', 'OXGP', 'no2', 'no3', 'nh4', 
                                                        'ph', 'po', 'bpk', 'turb', 'no']].replace({0.0: None, 0.00: None, 999.00: None, 999.0: None})
    
        mask_chem_true = ((df['sio3'].notnull()) | (df['po4'].notnull()) | (df['o2'].notnull()) | 
            (df['OXGP'].notnull()) | (df['no2'].notnull()) | (df['no3'].notnull()) | 
            (df['nh4'].notnull()) | (df['ph'].notnull()) | (df['po'].notnull()) | 
            (df['bpk'].notnull()) | (df['turb'].notnull()) | (df['no'].notnull()))
    
        df['pr_chemi'] = mask_chem_true
        horizon_df = df[['ship', 'reis', 'nst_g', 'gor', 't', 'sl', 'pr', 'pr_chemi']]
        chem_elem = df[['ship', 'reis', 'nst_g', 'gor', 'o2', 'po4', 'sio3', 'no2', 
                        'no3', 'ph', 'no', 'bpk', 'nh4', 'po', 'turb']]

        chem_elem = chem_elem.loc[mask_chem_true]

        return [horizon_df, chem_elem]              



