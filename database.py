"""
файл, который содержит функции для работы с базой данных.
"""
import psycopg2
from psycopg2 import extras
from tkinter import messagebox
import pandas as pd
from tkinter import filedialog
import configparser

class DatabaseUploader:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self, dbname, user, password, host):
        try:
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
            self.cursor = self.conn.cursor()
        except:
            print('Can`t establish connection to database')

        return [self.cursor, self.conn]

    # ============= Порейсовая выгрузка =============
    # предпросмотр Station
    def preload_datas_reis_station(self, ship_input, reis_input):
        config = configparser.ConfigParser()
        config.read('config.ini')
        try:
            get_conn_cur = DatabaseUploader.connect(self,
                                                    config['postgresql']['database'],
                                                    config['postgresql']['user'],
                                                    config['postgresql']['password'],
                                                    config['postgresql']['host'])

            connect = get_conn_cur[1]

            ship = tuple(ship_input.split(","))
            reis = tuple(reis_input.split(","))

            # ////////////////// stkl
            query_stkl = "SELECT stkl FROM station_2 WHERE ship IN %s and reis IN %s;"
            cursor_stkl = connect.cursor()
            cursor_stkl.execute(query_stkl, (ship, reis))
            stkls = []
            for row in cursor_stkl.fetchall():
                stkls.append(row[0])

            stkls_tuple = tuple(stkls)

            query_1 = """
            SELECT 
                station_2.ship, 
                station_2.reis, 
                station_2.nst_g, 
                station_2.datatime, 
                station_2.reis_time, 
                station_2.shir_dgr, 
                station_2.dolg_dgr, 
                station_2.glub, 
                horizon_2.gor,
                station_2.press, 
                station_2.t_air, 
                station_2.v_wind, 
                station_2.f_wind, 
                station_2.v_wave, 
                station_2.h_wave, 
                station_2.visibil, 
                station_2.cloud, 
                station_2.clo_nyr, 
                station_2.for_clo, 
                station_2.tr_water, 
                station_2.rzr1, 
                station_2.rzr2,
                station_2.weather, 
                station_2.st_operator, 
                station_2.interpolated
            FROM station_2 
            JOIN horizon_2 ON station_2.stkl = horizon_2.stkl
            WHERE station_2.stkl in %s
            ORDER BY 1, 2, 3, 9;
            """

            cursor_1 = connect.cursor()
            cursor_1.execute(query_1, (stkls_tuple,))
            rows_station = []
            for row in cursor_1.fetchall():
                rows_station.append(tuple(row))

            df_station = pd.DataFrame(rows_station, columns=['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                                                             'longitude', 'Depth', 'Horizon', 'Press', 'T_air', 'V_wind',
                                                             'F_wind', 'V_wave', 'H_wave', 'Visibil', 'Cloud', 'Clo_nyr',
                                                             'For_clo', 'Tr_water', 'Rzr1', 'Rzr2', 'Weather', 'Operator',
                                                             'Interpolated'])

            df_station['Press'] = df_station['Press'].apply(lambda x: int(x) if x is not None else x)
            df_station['V_wind'] = df_station['V_wind'].apply(lambda x: int(x) if x is not None else x)
            df_station['F_wind'] = df_station['F_wind'].apply(lambda x: int(x) if x is not None else x)
            df_station['V_wave'] = df_station['V_wave'].apply(lambda x: int(x) if x is not None else x)
            df_station['Visibil'] = df_station['Visibil'].apply(lambda x: int(x) if x is not None else x)
            df_station['Cloud'] = df_station['Cloud'].apply(lambda x: int(x) if x is not None else x)
            df_station['Clo_nyr'] = df_station['Clo_nyr'].apply(lambda x: int(x) if x is not None else x)
            df_station['Weather'] = df_station['Weather'].apply(lambda x: int(x) if x is not None else x)
            df_station['Horizon'] = df_station['Horizon'].apply(lambda x: int(x) if x is not None else x)
            df_station['Depth'] = df_station['Depth'].apply(lambda x: int(x) if x is not None else x)

            df_station['latitude'] = df_station['latitude'].apply(lambda x: '{:.3f}'.format(x))
            df_station['longitude'] = df_station['longitude'].apply(lambda x: '{:.3f}'.format(x))
            df_station['Time'] = df_station['Time'].apply(lambda x: x.strftime('%H:%M') if x is not None else None)

            self.table_down_station.model.df = df_station
            self.table_down_station.redraw()

        except Exception as e:
            messagebox.showinfo(title="Ошибка!", message=f"Ошибка - {e}")
            print(e)

    # предпросмотр Chem_elem + Horizon
    def preload_datas_reis_chem_elem_horizon(self, ship_input, reis_input, columns):
        config = configparser.ConfigParser()
        config.read('config.ini')

        get_conn_cur = DatabaseUploader.connect(self,
                                                config['postgresql']['database'],
                                                config['postgresql']['user'],
                                                config['postgresql']['password'],
                                                config['postgresql']['host'])
        connect = get_conn_cur[1]

        ship = tuple(ship_input.split(","))
        reis = tuple(reis_input.split(","))

        # ////////////////// stkl
        query_stkl = "SELECT stkl FROM station_2 WHERE ship IN %s and reis IN %s;"
        cursor_stkl = connect.cursor()
        cursor_stkl.execute(query_stkl, (ship, reis))
        stkls = []
        for row in cursor_stkl.fetchall():
            stkls.append(row[0])

        stkls_tuple = tuple(stkls)

        qur = """
        SELECT
            horizon_2.ship, 
            horizon_2.reis,
            horizon_2.nst_g,
            s.datatime,
            s.reis_time,
            s.shir_dgr,
            s.dolg_dgr,
            s.glub,
            horizon_2.gor as Horizon,
            horizon_2.t_water,
            horizon_2.sl_water,
            COALESCE(chem_elem_2.o2, null) as O2,
            COALESCE(chem_elem_2.po4, null) as po4,
            COALESCE(chem_elem_2.sio3, null) as sio3,
            COALESCE(chem_elem_2.no2, null) as no2,
            COALESCE(chem_elem_2.no3, null) as no3,
            COALESCE(chem_elem_2.ph, null) as ph,
            COALESCE(chem_elem_2.no, null) as no,
            COALESCE(chem_elem_2.bpk, null) as bpk,
            COALESCE(chem_elem_2.nh4, null) as nh4,
            COALESCE(chem_elem_2.po, null) as po,
            COALESCE(chem_elem_2.turb, null) as turb
        FROM horizon_2 
        LEFT JOIN chem_elem_2 ON horizon_2.stkl = chem_elem_2.stkl AND horizon_2.gor = chem_elem_2.gor
        LEFT JOIN station_2 s ON s.stkl = horizon_2.stkl 
        WHERE horizon_2.stkl IN %s
        ORDER BY 1,2,3,4,5,6,7,8,9;
        """

        cursor_2 = connect.cursor()
        cursor_2.execute(qur, (stkls_tuple,))
        rows_horizon = []
        for row in cursor_2.fetchall():
            rows_horizon.append(tuple(row))

        df_horiz_chem = pd.DataFrame(rows_horizon, columns=['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                                                            'longitude', 'Depth', 'Horizon', 'T', 'Sl', 'O2',
                                                            'Po4', 'Sio3', 'No2', 'No3', 'Ph', 'No', 'Bpk', 'Nh4',
                                                            'Po', 'Turb'])
        # указываем отображаемое кол-во знаков после запятой в этих колонках
        df_horiz_chem['Horizon'] = df_horiz_chem['Horizon'].apply(lambda x: int(x) if x is not None else x)
        df_horiz_chem['Depth'] = df_horiz_chem['Depth'].apply(lambda x: int(x) if x is not None else x)
        df_horiz_chem['latitude'] = df_horiz_chem['latitude'].apply(lambda x: '{:.3f}'.format(x))
        df_horiz_chem['longitude'] = df_horiz_chem['longitude'].apply(lambda x: '{:.3f}'.format(x))
        df_horiz_chem['Time'] = df_horiz_chem['Time'].apply(lambda x: x.strftime('%H:%M') if x is not None else None)
        df_horiz_chem['T'] = df_horiz_chem['T'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Sl'] = df_horiz_chem['Sl'].apply(lambda x: '{:.3f}'.format(x) if x is not None else None)
        df_horiz_chem['O2'] = df_horiz_chem['O2'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Po4'] = df_horiz_chem['Po4'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Sio3'] = df_horiz_chem['Sio3'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No2'] = df_horiz_chem['No2'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No3'] = df_horiz_chem['No3'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No'] = df_horiz_chem['No'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Bpk'] = df_horiz_chem['Bpk'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Nh4'] = df_horiz_chem['Nh4'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Po'] = df_horiz_chem['Po'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Turb'] = df_horiz_chem['Turb'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)

        self.table_down_chem_elem.model.df = df_horiz_chem.loc[:, columns]
        self.table_down_chem_elem.redraw()

    # выгрузка данных из station в CSV
    def down_reis_station(self):

        df_station_get = self.table_down_station.model.df

        file_path = filedialog.asksaveasfilename(defaultextension='.csv')
        if not file_path:
            return
        df_station_get.to_csv(file_path, index=False, header=True,
                              decimal='.', float_format='%.2f', sep=';')
        messagebox.showinfo(title="Данные выгружены!", message="Данные выгружены!")

    # выгрузка данных из horiozn + chem_elem в CSV
    def get_reis_horiz_chem(self, columns):

        df_horiz_chem_get = self.table_down_chem_elem.model.df

        file_path = filedialog.asksaveasfilename(defaultextension='.csv')
        if not file_path:
            return
        df_horiz_chem_get.loc[:, columns].to_csv(file_path, index=False, header=True,
                                                 decimal='.',
                                                 float_format='%.2f',
                                                 sep=';')
        messagebox.showinfo(title="Данные выгружены!", message="Данные выгружены!")

    # ============= // Порейсовая выгрузка =============

    # ============= Выгрзука и предпросмотр по РАЗРЕЗАМ =============
    # предпросмотр таблицы station
    def preload_datas_rzrs_station(self, rzr1_input):
        config = configparser.ConfigParser()
        config.read('config.ini')

        get_conn_cur = DatabaseUploader.connect(self,
                                                config['postgresql']['database'],
                                                config['postgresql']['user'],
                                                config['postgresql']['password'],
                                                config['postgresql']['host'])
        connect = get_conn_cur[1]

        rzr1 = rzr1_input.split(",")

        # ////////////////// stkl
        query_stkl = "SELECT stkl FROM station_2 WHERE rzr1 IN %s;"
        cursor_stkl = connect.cursor()
        cursor_stkl.execute(query_stkl, (tuple(rzr1),))
        stkls = []
        for row in cursor_stkl.fetchall():
            stkls.append(row[0])
        stkls_tuple = tuple(stkls)

        query_1 = """
        SELECT 
            station_2.ship, 
            station_2.reis, 
            station_2.nst_g, 
            station_2.datatime, 
            station_2.reis_time, 
            station_2.shir_dgr, 
            station_2.dolg_dgr, 
            station_2.glub, 
            horizon_2.gor,
            station_2.press, 
            station_2.t_air,
            station_2.v_wind, 
            station_2.f_wind, 
            station_2.v_wave, 
            station_2.h_wave, station_2.visibil, station_2.cloud, station_2.clo_nyr, station_2.for_clo, station_2.tr_water, 
            station_2.rzr1, station_2.rzr2, station_2.weather, station_2.st_operator, station_2.interpolated
        FROM station_2 
        JOIN horizon_2 ON station_2.stkl = horizon_2.stkl
        WHERE station_2.stkl in %s
        ORDER BY 1, 2, 3, 9;
        """

        cursor_1 = connect.cursor()
        cursor_1.execute(query_1, (tuple(stkls_tuple),))
        rows_station = []
        for row in cursor_1.fetchall():
            rows_station.append(tuple(row))

        cols = ['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                'longitude', 'Depth', 'Horizon', 'Press', 'T_air', 'V_wind',
                'F_wind', 'V_wave', 'H_wave', 'Visibil', 'Cloud', 'Clo_nyr',
                'For_clo', 'Tr_water', 'Rzr1', 'Rzr2', 'Weather', 'Operator',
                'Interpolated']

        df_station = pd.DataFrame(rows_station, columns=cols)

        df_station['latitude'] = df_station['latitude'].apply(lambda x: '{:.3f}'.format(x))
        df_station['longitude'] = df_station['longitude'].apply(lambda x: '{:.3f}'.format(x))
        df_station['Time'] = df_station['Time'].apply(lambda x: x.strftime('%H:%M') if x is not None else None)

        df_station['Press'] = df_station['Press'].apply(lambda x: int(x) if x is not None else x)
        df_station['Horizon'] = df_station['Horizon'].apply(lambda x: int(x) if x is not None else x)
        df_station['Depth'] = df_station['Depth'].apply(lambda x: int(x) if x is not None else x)
        df_station['V_wind'] = df_station['V_wind'].apply(lambda x: int(x) if x is not None else x)
        df_station['F_wind'] = df_station['F_wind'].apply(lambda x: int(x) if x is not None else x)
        df_station['V_wave'] = df_station['V_wave'].apply(lambda x: int(x) if x is not None else x)
        df_station['Visibil'] = df_station['Visibil'].apply(lambda x: int(x) if x is not None else x)
        df_station['Cloud'] = df_station['Cloud'].apply(lambda x: int(x) if x is not None else x)
        df_station['Clo_nyr'] = df_station['Clo_nyr'].apply(lambda x: int(x) if x is not None else x)
        df_station['Weather'] = df_station['Weather'].apply(lambda x: int(x) if x is not None else x)
        self.table_down_station_rzr.model.df = df_station
        self.table_down_station_rzr.redraw()

    # предпросмотр таблицы horizon
    def preload_datas_rzrs_horizon(self, rzr1_input, columns):
        config = configparser.ConfigParser()
        config.read('config.ini')

        get_conn_cur = DatabaseUploader.connect(self,
                                                config['postgresql']['database'],
                                                config['postgresql']['user'],
                                                config['postgresql']['password'],
                                                config['postgresql']['host'])
        connect = get_conn_cur[1]

        rzr1 = rzr1_input.split(",")

        query_2 = """
        SELECT    
            horizon_2.ship, 
            horizon_2.reis,
            horizon_2.nst_g,
            s.datatime,
            s.reis_time,
            s.shir_dgr,
            s.dolg_dgr,
            s.glub,
            horizon_2.gor as Horizon,
            horizon_2.t_water,
            horizon_2.sl_water,
            COALESCE(chem_elem_2.o2, null) as O2,
            COALESCE(chem_elem_2.po4, null) as po4,
            COALESCE(chem_elem_2.sio3, null) as sio3,
            COALESCE(chem_elem_2.no2, null) as no2,
            COALESCE(chem_elem_2.no3, null) as no3,
            COALESCE(chem_elem_2.ph, null) as ph,
            COALESCE(chem_elem_2.no, null) as no,
            COALESCE(chem_elem_2.bpk, null) as bpk,
            COALESCE(chem_elem_2.nh4, null) as nh4,
            COALESCE(chem_elem_2.po, null) as po,
            COALESCE(chem_elem_2.turb, null) as turb
        FROM horizon_2 
        LEFT JOIN chem_elem_2 ON horizon_2.stkl = chem_elem_2.stkl AND horizon_2.gor = chem_elem_2.gor
        LEFT JOIN station_2 s ON s.stkl = horizon_2.stkl 
        WHERE s.rzr1 IN %s
        ORDER BY 1,2,3,4,5,6,7,8,9;
        """

        cursor_2 = connect.cursor()
        cursor_2.execute(query_2, (tuple(rzr1),))
        rows_horizon = []
        for row in cursor_2.fetchall():
            rows_horizon.append(tuple(row))

        df_horiz_chem = pd.DataFrame(rows_horizon, columns=['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                                                            'longitude', 'Depth', 'Horizon', 'T', 'Sl', 'O2',
                                                            'Po4', 'Sio3', 'No2', 'No3', 'Ph', 'No', 'Bpk', 'Nh4',
                                                            'Po', 'Turb'])
        # указываем отображаемое кол-во знаков после запятой в этих колонках
        df_horiz_chem['Horizon'] = df_horiz_chem['Horizon'].apply(lambda x: int(x) if x is not None else x)
        df_horiz_chem['Depth'] = df_horiz_chem['Depth'].apply(lambda x: int(x) if x is not None else x)
        df_horiz_chem['latitude'] = df_horiz_chem['latitude'].apply(lambda x: '{:.3f}'.format(x))
        df_horiz_chem['longitude'] = df_horiz_chem['longitude'].apply(lambda x: '{:.3f}'.format(x))
        df_horiz_chem['Time'] = df_horiz_chem['Time'].apply(lambda x: x.strftime('%H:%M') if x is not None else None)
        df_horiz_chem['T'] = df_horiz_chem['T'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Sl'] = df_horiz_chem['Sl'].apply(lambda x: '{:.3f}'.format(x) if x is not None else None)
        df_horiz_chem['O2'] = df_horiz_chem['O2'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Po4'] = df_horiz_chem['Po4'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Sio3'] = df_horiz_chem['Sio3'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No2'] = df_horiz_chem['No2'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No3'] = df_horiz_chem['No3'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['No'] = df_horiz_chem['No'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Bpk'] = df_horiz_chem['Bpk'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Nh4'] = df_horiz_chem['Nh4'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Po'] = df_horiz_chem['Po'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)
        df_horiz_chem['Turb'] = df_horiz_chem['Turb'].apply(lambda x: '{:.2f}'.format(x) if x is not None else None)

        self.table_down_horizon_chem_rzr.model.df = df_horiz_chem.loc[:, columns]
        self.table_down_horizon_chem_rzr.redraw()

    # выгрузка данных в CSV из station
    def get_station_rzr(self):
        df_station_rzr = self.table_down_station_rzr.model.df

        file_path = filedialog.asksaveasfilename(defaultextension='.csv')
        if not file_path:
            return
        df_station_rzr.to_csv(file_path, index=False, header=True,
                              decimal='.', float_format='%.2f', sep=';')
        messagebox.showinfo(title="Данные выгружены!", message="Данные выгружены!")

    # выгрузка данных в CSV из horizon
    def get_horizon_rzr(self):
        df_horizon_rzr = self.table_down_horizon_chem_rzr.model.df

        file_path = filedialog.asksaveasfilename(defaultextension='.csv')
        if not file_path:
            return
        df_horizon_rzr.to_csv(file_path, index=False, header=True,
                              decimal='.', float_format='%.2f', sep=';')
        messagebox.showinfo(title="Данные выгружены!", message="Данные выгружены!")

    def upload_all_tables(self, notebook):
        config = configparser.ConfigParser()
        config.read('config.ini')

        get_conn_curs = DatabaseUploader.connect(self,
                                                 config['postgresql']['database'],
                                                 config['postgresql']['user'],
                                                 config['postgresql']['password'],
                                                 config['postgresql']['host'])
        cursor = get_conn_curs[0]
        connect = get_conn_curs[1]

        if notebook == "Загрузка данных в БД":
            try:
# //////////////////////////station
                ship = self.new_df['ship'][0]
                reis = self.new_df['reis'][0]

                self.upload_query = f"""
                DELETE FROM station_2 WHERE ship = '{ship}' and reis = {reis};
                DELETE FROM horizon_2 WHERE ship = '{ship}' and reis = {reis};
                DELETE FROM chem_elem_2 WHERE ship = '{ship}' and reis = {reis};
                """

                cursor.execute(self.upload_query)

                self.stkl_current = f"""
                SELECT MAX(stkl) from station_2;
                """

                cursor.execute(self.stkl_current)

                max_stkl = cursor.fetchone()[0]
                if max_stkl is None:
                    max_stkl = 0
                
                self.sql_station = f"""
                INSERT INTO station_2 (
                    stkl, 
                    ship, 
                    reis, 
                    nst_g, 
                    nst_p,
                    nst_b, 
                    nst_l,
                    nst_yr, 
                    trl,
                    datatime, 
                    rnloc, 
                    rnzon, 
                    rnikes, 
                    shir_dgr, 
                    dolg_dgr, 
                    shir_dsec, 
                    dolg_dsec, 
                    glub,
                    press, 
                    t_air, 
                    v_wind, 
                    f_wind, 
                    v_wave, 
                    h_wave, 
                    visibil, 
                    cloud, 
                    clo_nyr, 
                    for_clo,
                    tr_water, 
                    rzr1, 
                    rzr2, 
                    weather, 
                    st_operator, 
                    interpolated, 
                    reis_time)
                VALUES %s;
                """

                df_station = self.table.model.df
                df_station[['nst_p', 'nst_b', 'nst_l', 'nst_yr', 'trl', 
                            'press', 't_air', 'v_wind', 'f_wind', 'v_wave',
                            'h_wave', 'visibil', 'cloud', 'clo_nyr', 'for_clo',
                            'tr_water', 'rzr1', 'rzr2', 'weather', 'operator']] = df_station[['nst_p', 'nst_b', 'nst_l', 'nst_yr', 'trl', 
                                                                                                'press', 't_air', 'v_wind', 'f_wind', 'v_wave',
                                                                                                'h_wave', 'visibil', 'cloud', 'clo_nyr', 'for_clo',
                                                                                                'tr_water', 'rzr1', 'rzr2', 'weather', 'operator']].fillna(0).replace({0.0: None, 0.00: None})
                if 'stkl' in df_station:
                    df_station = df_station.drop('stkl', axis=1)

                df_station.insert(0, 'stkl', None)
                df_station['stkl'] = range(max_stkl + 1, max_stkl+1 + len(df_station))

                time_column = df_station["st_time"]
                df_station = df_station.drop("st_time", axis=1)
                df_station.insert(34, 'reis_time', time_column)

                self.values_station = [tuple(x) for x in df_station.to_numpy()]
                print(self.values_station)
                print(df_station)

                psycopg2.extras.execute_values(cursor, self.sql_station, self.values_station)

# //////////////////////////horizon
                df_horizon = self.table_uploading_horizon.model.df

                self.get_stkls_query = f"""
                SELECT stkl, ship, reis, nst_g FROM station_2
                WHERE ship='{ship}' AND reis={reis};
                """

                cursor.execute(self.get_stkls_query)

                current_hrkl_horiz = "SELECT MAX(hrkl) from horizon_2;"
                cursor.execute(current_hrkl_horiz)
                max_hrkl_horizon = cursor.fetchone()[0]
                if max_hrkl_horizon is None:
                    max_hrkl_horizon = 0
                
                df_stkls = pd.read_sql_query(self.get_stkls_query, connect)

                df_stkls['stkl'] = df_stkls["stkl"].astype('Int64')
                df_stkls['ship'] = df_stkls["ship"].astype('str')
                df_stkls['reis'] = df_stkls["reis"].astype('Int64')
                df_stkls['nst_g'] = df_stkls["nst_g"].astype('Int64')

                df_horizon['ship'] = df_horizon["ship"].astype('str')
                df_horizon['reis'] = df_horizon["reis"].astype('Int64')
                df_horizon['nst_g'] = df_horizon["nst_g"].astype('Int64')

                merged_df = df_horizon.merge(df_stkls, on=['ship', 'reis', 'nst_g'], how='outer')

                df_horizon = merged_df[['stkl', 'ship', 'reis', 'nst_g', 'gor', 't', 'sl', 'pr', 'pr_chemi']]

                if 'hrkl' in df_horizon:
                    df_horizon = df_horizon.drop('hrkl', axis = 1)
                
                df_horizon.insert(0, 'hrkl', None)
                df_horizon['hrkl'] = range(max_hrkl_horizon + 1, max_hrkl_horizon+1 + len(df_horizon))

                self.sql_horizon = f"""
                INSERT INTO horizon_2 (
                    hrkl, 
                    stkl, 
                    ship, 
                    reis, 
                    nst_g, 
                    gor, 
                    t_water, 
                    sl_water, 
                    pr, 
                    pr_chemi)
                VALUES %s
                ON CONFLICT (
                    ship, 
                    reis, 
                    nst_g, 
                    gor) DO UPDATE SET
                stkl = EXCLUDED.stkl;
                """

                self.values_horizon = [tuple(x) for x in df_horizon.to_numpy()]
                psycopg2.extras.execute_values(cursor, self.sql_horizon, self.values_horizon)

# //////////////////////////chem_elem
                df_chem_elem = self.table_chem_elem.model.df
                df_chem_elem['ship'] = df_chem_elem["ship"].astype('str')
                df_chem_elem['reis'] = df_chem_elem["reis"].astype('Int64')
                df_chem_elem['nst_g'] = df_chem_elem["nst_g"].astype('Int64')

                chem_stkls = {'stkl': [], 'ship': [], 'reis': [], 'nst_g': []}
                for i, row in df_chem_elem.iterrows():
                    cursor.execute(f"SELECT stkl, ship, reis, nst_g FROM station_2 WHERE ship='{ship}' AND reis={reis} AND nst_g={row['nst_g']};")
                    for k in cursor:
                        chem_stkls["stkl"].append(k[0])
                        chem_stkls["ship"].append(k[1])
                        chem_stkls["reis"].append(k[2])
                        chem_stkls["nst_g"].append(k[3])

                chem_stkls_df = pd.DataFrame(chem_stkls)
                chem_stkls_df['stkl'] = chem_stkls_df["stkl"].astype('Int64')
                chem_stkls_df['ship'] = chem_stkls_df["ship"].astype('str')
                chem_stkls_df['reis'] = chem_stkls_df["reis"].astype('Int64')
                chem_stkls_df['nst_g'] = chem_stkls_df["nst_g"].astype('Int64') 

                merged_df_chemy = df_chem_elem.merge(chem_stkls_df, on=['ship', 'reis', 'nst_g'], how='outer')
                merged_df_chemy.drop_duplicates()
                df_chem = merged_df_chemy[['stkl', 'ship', 'reis', 'nst_g', 'gor', 'o2', 'po4', 'sio3', 'no2', 'no3', 'ph', 'no', 'bpk', 'nh4', 'po', 'turb']]
                
                df_chem['gor'] = df_chem['gor'].fillna(0)
                df_chem['o2'] = df_chem['o2'].fillna(0)
                df_chem['po4'] = df_chem['po4'].fillna(0)
                df_chem['sio3'] = df_chem['sio3'].fillna(0)
                df_chem['no2'] = df_chem['no2'].fillna(0)
                df_chem['no3'] = df_chem['no3'].fillna(0)
                df_chem['ph'] = df_chem['ph'].fillna(0)
                df_chem['no'] = df_chem['no'].fillna(0)
                df_chem['bpk'] = df_chem['bpk'].fillna(0)
                df_chem['nh4'] = df_chem['nh4'].fillna(0)
                df_chem['po'] = df_chem['po'].fillna(0)
                df_chem['turb'] = df_chem['turb'].fillna(0)

                mask_chem_notna = ((df_chem['gor'] != 0) | (df_chem['sio3'] != 0) |
                                   (df_chem['po4'] != 0) | (df_chem['o2'] != 0) |
                                   (df_chem['no2'] != 0) | (df_chem['no3'] != 0) |
                                   (df_chem['nh4'] != 0) | (df_chem['ph'] != 0) | (df_chem['po'] != 0) |
                                   (df_chem['bpk'] != 0) | (df_chem['turb'] != 0) | (df_chem['no'] != 0))

                df_chem = df_chem.loc[mask_chem_notna]
                df_chem[['po4', 'sio3', 'no2', 'no3', 'ph', 
                         'no', 'bpk', 'nh4', 'po', 'turb']] = df_chem[['po4', 'sio3', 'no2', 'no3', 'ph', 
                                                                       'no', 'bpk', 'nh4', 'po', 'turb']].replace({0: None})
                df_chem = df_chem.drop_duplicates()

                curent_hrkl_chem = f"SELECT MAX(hrkl) from chem_elem_2;"
                cursor.execute(curent_hrkl_chem)
                max_hrkl_chem = cursor.fetchone()[0]
                if max_hrkl_chem is None:
                    max_hrkl_chem = 0

                if 'hrkl' in df_chem:
                    df_chem = df_chem.drop('hrkl', axis=1)

                df_chem.insert(0, 'hrkl', None)
                df_chem['hrkl'] = range(max_hrkl_chem+1, max_hrkl_chem+1 + len(df_chem))

                self.sql_chem_elem = f"""
                INSERT INTO chem_elem_2 (
                    hrkl, 
                    stkl, 
                    ship, 
                    reis, 
                    nst_g, 
                    gor, 
                    o2, 
                    po4, 
                    sio3, 
                    no2, 
                    no3, 
                    ph, 
                    no, 
                    bpk, 
                    nh4, 
                    po, 
                    turb)
                VALUES %s
                ON CONFLICT (
                    ship, 
                    reis, 
                    nst_g, 
                    gor) DO UPDATE SET
                stkl = EXCLUDED.stkl;
                """

                self.val = [tuple(x) for x in df_chem.to_numpy()]

                psycopg2.extras.execute_values(cursor, self.sql_chem_elem, self.val)

                messagebox.showinfo("Загрузка данных", "Данные загружены!")
                connect.commit()

                cursor.close()
                connect.close()

            except Exception as e:
                print(f"Не работает. {e}!")
                messagebox.showinfo("Загрузка данных", f"Не работает. {e}!")              

    def upload_data(self, notebook):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.cursor_down = DatabaseUploader.connect(self,
                                                    config['postgresql']['database'],
                                                    config['postgresql']['user'],
                                                    config['postgresql']['password'],
                                                    config['postgresql']['host'])

        if notebook == "Station":
            # Получение максимального значения ключа
            self.cursor_down[0].execute("SELECT MAX(stkl) FROM station_t;")
            self.max_id = self.cursor_down[0].fetchone()[0]
            if self.max_id is None:
                self.max_id = 0

            self.new_df.insert(0, 'stkl', None)
            
            self.new_df['stkl'] = range(self.max_id + 1, self.max_id + 1 + len(self.new_df))

            try:
        # # # Формируем SQL-запрос на вставку данных                
                self.sql = f"""INSERT INTO station_t (
                                    stkl, ship, reis, nst_g, nst_p, nst_b, nst_l, nst_yr, trl, 
                                    datatime, rnloc, rnzon, rnikes, shir_dgr, dolg_dgr, shir_dsec, 
                                    dolg_dsec, glub, press, t_air, v_wind, f_wind, v_wave, h_wave, 
                                    visibil, cloud, clo_nyr, for_clo, tr_water, rzr1, rzr2, weather, 
                                    st_operator, interpolated) VALUES %s
                                ON CONFLICT (
                                    ship, reis, nst_g) 
                                DO UPDATE SET
                                    stkl = EXCLUDED.stkl,
                                    ship = EXCLUDED.ship,
                                    reis = EXCLUDED.reis,
                                    nst_g = EXCLUDED.nst_g,
                                    nst_p = EXCLUDED.nst_p,
                                    nst_b = EXCLUDED.nst_b,
                                    nst_l = EXCLUDED.nst_l,
                                    nst_yr = EXCLUDED.nst_yr,
                                    datatime = EXCLUDED.datatime,
                                    rnloc = EXCLUDED.rnloc,
                                    rnzon = EXCLUDED.rnzon,
                                    rnikes = EXCLUDED.rnikes,
                                    shir_dgr = EXCLUDED.shir_dgr,
                                    dolg_dgr = EXCLUDED.dolg_dgr,
                                    shir_dsec = EXCLUDED.shir_dsec,
                                    dolg_dsec = EXCLUDED.dolg_dsec,
                                    press = EXCLUDED.press,
                                    t_air = EXCLUDED.t_air,
                                    v_wind = EXCLUDED.v_wind,
                                    f_wind = EXCLUDED.f_wind,
                                    v_wave = EXCLUDED.v_wave,
                                    h_wave = EXCLUDED.h_wave,
                                    visibil = EXCLUDED.visibil,
                                    cloud = EXCLUDED.cloud,
                                    clo_nyr = EXCLUDED.clo_nyr,
                                    for_clo = EXCLUDED.for_clo,
                                    tr_water = EXCLUDED.tr_water,
                                    rzr1 = EXCLUDED.rzr1,
                                    rzr2 = EXCLUDED.rzr2,
                                    weather = EXCLUDED.weather,
                                    st_operator = EXCLUDED.st_operator,
                                    interpolated = EXCLUDED.interpolated;
                            """

        # # # # Преобразуем данные из DataFrame в список кортежей
                self.values = [tuple(x) for x in self.new_df.to_numpy()]

        # # # # Используем метод execute_values() для выполнения вставки
                with self.cursor_down[0] as cur:
                    extras.execute_values(cur, self.sql, self.values)   
            
                self.cursor_down[1].commit()     
                self.cursor_down[1].close()
                messagebox.showinfo("Загрузка данных", "Загрузка данных в базу завершена успешно!")

            except psycopg2.errors.UniqueViolation as e:
                messagebox.showerror("Ошибка загрузки данных", f"Возникла ошибка при загрузке данных: {e}")

        elif notebook == "Horizon":
            self.cursor_down[0].execute("SELECT MAX(hrkl) FROM horizon;")
            self.max_id = self.cursor_down[0].fetchone()[0]
            if self.max_id is None:
                self.max_id = 0

            self.horizon_table.insert(0, 'hrkl', None)
            self.horizon_table['hrkl'] = range(self.max_id + 1, self.max_id + 1 + len(self.horizon_table))

            try:
                self.sql = f"""
                INSERT INTO horizon (
                    hrkl, 
                    stkl, 
                    ship, 
                    reis, 
                    nst_g, 
                    gor, 
                    t_water, 
                    sl_water, 
                    pr, 
                    pr_chemi) 
                VALUES %s 
                ON CONFLICT (
                    ship, 
                    reis, 
                    nst_g, 
                    gor) DO UPDATE SET
                stkl = EXCLUDED.stkl,
                pr_chemi = EXCLUDED.pr_chemi;
                """
                self.values = [tuple(x) for x in self.horizon_table.to_numpy()]

                with self.cursor_down[0] as cur:
                    extras.execute_values(cur, self.sql, self.values)   
                self.cursor_down[1].commit()     
                self.cursor_down[1].close()
                messagebox.showinfo("Загрузка данных", "Загрузка данных в базу завершена успешно!")

            except psycopg2.errors.UniqueViolation as e:
                messagebox.showerror("Ошибка загрузки данных", f"Возникла ошибка при загрузке данных: {e}")

        elif notebook == "Chem elem":
            self.cursor_down[0].execute("SELECT MAX(hrkl) FROM chem_elem;")
            self.max_id = self.cursor_down[0].fetchone()[0]
            if self.max_id is None:
                self.max_id = 0

            self.chem_elem_table.insert(0, 'hrkl', None)
            self.chem_elem_table['hrkl'] = range(self.max_id + 1, self.max_id + 1 + len(self.chem_elem_table))

            try:
                self.sql = f"""
                INSERT INTO chem_elem (
                    hrkl, 
                    stkl, 
                    ship, 
                    reis, 
                    nst_g, 
                    gor, 
                    o2, 
                    po4, 
                    sio3, 
                    no2, 
                    no3, 
                    ph, 
                    no, 
                    bpk, 
                    nh4, 
                    po, 
                    turb) 
                VALUES %s 
                ON CONFLICT (
                    ship,
                    reis, 
                    nst_g, 
                    gor) DO UPDATE SET
                stkl = EXCLUDED.stkl;
                """
                self.values = [tuple(x) for x in self.chem_elem_table.to_numpy()]

                with self.cursor_down[0] as cur:
                    extras.execute_values(cur, self.sql, self.values)

                self.cursor_down[1].commit()     
                self.cursor_down[1].close()

                messagebox.showinfo("Загрузка данных", "Загрузка данных в базу завершена успешно!")

            except psycopg2.errors.UniqueViolation as e:
                messagebox.showerror("Ошибка загрузки данных", f"Возникла ошибка при загрузке данных: {e}")
