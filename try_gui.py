"""
главный файл, содержащий интерфейс приложения
"""

import threading
import tkinter as tk
import customtkinter
from tkinter import ttk
from pandastable import Table, TableModel
import pandas as pd
from tkinter import filedialog, scrolledtext, messagebox
import subprocess
from check_datas import CheckDatas
from export_files import ExportingFiles
from test import FileProcessor
from database import DatabaseUploader
from tkinter import PhotoImage
from prs_processing import PrsProcessor

customtkinter.set_appearance_mode("System")  # Modes: "System" (standard), "Dark", "Light"
customtkinter.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"

class App:
    def __init__(self, master):

        self.master = master
        self.master.title("My app")

        # configure window
        self.master.geometry(f"{1100}x{920}")

        # configure grid layout (4x4)
        self.master.grid_columnconfigure(1, weight=1)
        self.master.grid_columnconfigure((2, 3), weight=0)
        self.master.grid_rowconfigure((0, 1, 2, 3, 4), weight=1)

        self.notebook = ttk.Notebook(self.master)

        # ---------------------------------------------- STATION ---------------------------------------------
        self.stat_img = PhotoImage(file="./imgs/station.png")
        # Создаем вкладку "station"
        self.station_frame = ttk.Frame(self.notebook)
        self.station_frame.grid_columnconfigure(1, weight=1)
        self.station_frame.grid_columnconfigure((2, 3), weight=0)
        self.station_frame.grid_rowconfigure((0, 1, 2, 3, 4), weight=1)

        self.notebook.add(self.station_frame, text="Station", image=self.stat_img, compound=tk.LEFT)

        # create sidebar frame with widgets
        self.sidebar_frame = customtkinter.CTkFrame(self.station_frame, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_frame.grid_rowconfigure(7, weight=1)

        self.menu_bar = tk.Menu(self.master)
        self.file_menu = tk.Menu(self.menu_bar, tearoff=0)
        self.file_menu.add_command(label="Выбрать PRS-файлы", command=self.select_folder_prs)
        self.file_menu.add_command(label="Выбрать miniPRS-файлы", command=self.select_folder_mini)
        self.menu_bar.add_cascade(label="Файлы", menu=self.file_menu)
        self.master.config(menu=self.menu_bar)

        self.label_selected_files = customtkinter.CTkLabel(master=self.sidebar_frame, text="Выбранные файлы (PRS):")
        self.label_selected_files.grid(row=5, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.selected_files_text = tk.Listbox(self.sidebar_frame, selectmode=tk.SINGLE, bg='#FFFFFF', fg='#404040', height=30, width=50)
        self.selected_files_text.grid(row=6, column=0, padx=20, sticky='nw')
        self.selected_files_text.bind("<Double-Button-1>", self.open_file)

        self.dict_test = {'A': [1, 2, 3], 'B': [1, 2, 3]}
        self.df = pd.DataFrame(self.dict_test)

        self.frame_table = customtkinter.CTkFrame(self.station_frame, corner_radius=0)
        self.frame_table.grid(row=0, column=1, padx=(10, 10), sticky="nsew")
        self.table = Table(self.frame_table, dataframe=self.df, showtoolbar=True, showstatusbar=True)
        self.table.show()
        self.table.redraw()

        self.label_report = customtkinter.CTkLabel(master=self.station_frame, text="Отчет об ошибках:")
        self.label_report.grid(row=2, column=1, columnspan=1, ipadx=10, pady=(10, 0), sticky="nw")

        self.file_report_text = customtkinter.CTkTextbox(self.station_frame)
        self.file_report_text.grid(row=3, column=1, padx=(10, 20), pady=(0, 20), sticky="nsew")

        self.presission_button = customtkinter.CTkButton(self.sidebar_frame, corner_radius=5, height=25, width=300,
                                                         border_spacing=10,
                                                         text='Обработать', command=self.read_files)
        self.presission_button.grid(row=0, column=0, padx=(10, 20), pady=(50, 20), sticky="nw")

        self.report_button = customtkinter.CTkButton(self.sidebar_frame, corner_radius=5, height=25, width=300,
                                                     border_spacing=10,
                                                     text='Проверить значения', command=self.call_check_range)
        self.report_button.grid(row=1, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.export_button = customtkinter.CTkButton(self.sidebar_frame, corner_radius=5, height=25, width=300,
                                                     border_spacing=10,
                                                     text='Экспорт в CSV', command=self.export_results)
        self.export_button.grid(row=2, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.export_rep_button = customtkinter.CTkButton(self.sidebar_frame, corner_radius=5, height=25, width=300,
                                                         border_spacing=10,
                                                         text='Отчет об ошибке (DOCX)', command=self.export_report)
        self.export_rep_button.grid(row=3, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.progressbar_station = ttk.Progressbar(self.sidebar_frame, orient="horizontal",
                                                   length=200, mode="determinate")
        self.progressbar_station.grid(row=4, column=0, columnspan=1, padx=(10, 20),
                                      pady=(10, 0), sticky="nsew")

        # ----------------------------------------------- HORIZON ------------------------------------------------------
        self.horizon_img = PhotoImage(file="./imgs/horizon.png")

        # Создаем вкладку "horizon"
        self.horizon_frame = ttk.Frame(self.notebook)

        self.horizon_frame.grid_columnconfigure(1, weight=1)
        self.horizon_frame.grid_columnconfigure((2, 3), weight=0)
        self.horizon_frame.grid_rowconfigure((0, 1, 2, 3, 4, 5, 6, 7, 8), weight=1)

        self.notebook.add(self.horizon_frame, text="Horizon", state="normal", image=self.horizon_img, compound=tk.LEFT)

        # create sidebar frame with widgets
        self.sidebar_horizon_frame = customtkinter.CTkFrame(self.horizon_frame, corner_radius=0)
        self.sidebar_horizon_frame.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_horizon_frame.grid_rowconfigure(15, weight=1)

        self.label_selected_files_horizon = customtkinter.CTkLabel(master=self.sidebar_horizon_frame,
                                                                   text="Выбранные файлы (PRS):")
        self.label_selected_files_horizon.grid(row=7, column=0, columnspan=1, padx=20, sticky="nw")

        self.selected_files_text_horizon = tk.Listbox(self.sidebar_horizon_frame, selectmode=tk.SINGLE, bg='#FFFFFF',
                                                      fg='#404040', height=15, width=50)
        self.selected_files_text_horizon.grid(row=8, column=0, padx=20, sticky='nw')
        self.selected_files_text_horizon.bind("<Double-Button-1>", self.open_file_horizon)

        self.label_selected_files_horizon_mini = customtkinter.CTkLabel(master=self.sidebar_horizon_frame,
                                                                   text="Выбранные файлы (mini PRS):")
        self.label_selected_files_horizon_mini.grid(row=9, column=0, columnspan=1, padx=20, sticky="nw")

        self.selected_files_text_horizon_mini = tk.Listbox(self.sidebar_horizon_frame, selectmode=tk.SINGLE,
                                                           bg='#FFFFFF', fg='#404040', height=15, width=50)
        self.selected_files_text_horizon_mini.grid(row=10, column=0, padx=20, sticky='nw')
        self.selected_files_text_horizon_mini.bind("<Double-Button-1>", self.open_file_horizon_mini)

        self.df_horizon = pd.DataFrame(self.dict_test)

        self.frame_table_horizon_prs = customtkinter.CTkFrame(self.horizon_frame, corner_radius=0)
        self.frame_table_horizon_prs.grid(row=0, column=1, padx=(10, 0), sticky="w")

        self.table_horizon_prs = Table(self.frame_table_horizon_prs, dataframe=self.df_horizon, showtoolbar=True,
                                       showstatusbar=True)
        self.table_horizon_prs.show()
        self.table_horizon_prs.redraw()

        self.frame_table_horizon_Miniprs = customtkinter.CTkFrame(self.horizon_frame, corner_radius=0)
        self.frame_table_horizon_Miniprs.grid(row=0, column=2, padx=(0, 10), sticky="ew")

        self.table_horizon_prs_Miniprs = Table(self.frame_table_horizon_Miniprs, dataframe=self.df_horizon,
                                               showtoolbar=True, showstatusbar=True)
        self.table_horizon_prs_Miniprs.show()
        self.table_horizon_prs_Miniprs.redraw()

        self.frame_table_horizon_all = customtkinter.CTkFrame(self.horizon_frame, corner_radius=0)
        self.frame_table_horizon_all.grid(row=1, column=1, columnspan=2, padx=(0, 10), sticky="ew")

        self.table_horizon_prs_all = Table(self.frame_table_horizon_all, dataframe=self.df_horizon, showtoolbar=True,
                                           showstatusbar=True)
        self.table_horizon_prs_all.show()
        self.table_horizon_prs_all.redraw()

        self.label_report_horizon = customtkinter.CTkLabel(master=self.horizon_frame, text="Отчет об ошибках:")
        self.label_report_horizon.grid(row=3, column=1, columnspan=2, ipadx=10,
                                       pady=(10, 0), sticky="nw")

        self.file_report_text_horizon = customtkinter.CTkTextbox(self.horizon_frame)
        self.file_report_text_horizon.grid(row=4, column=1, columnspan=2, padx=(10, 20),
                                           pady=(0, 20), sticky="nsew")

        self.presission_button_horizon_prs = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5,
                                                                     height=25, width=300, border_spacing=10,
                                                                     text='Обработать PRS', fg_color=("#4040ff"),
                                                                     hover_color=("#4020e0"),
                                                                     command=lambda: self.read_and_preproc_prs_miniPrs(
                                                                         "Обработать PRS"))
        self.presission_button_horizon_prs.grid(row=0, column=0, padx=(10, 20), pady=(50, 0), sticky="nw")

        self.presission_button_horizon_mini = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5,
                                                                      height=25, width=300, border_spacing=10,
                                                                      text='Обработать Mini PRS', fg_color=("#4040ff"),
                                                                      hover_color=("#4020e0"),
                                                                      command=lambda: self.read_and_preproc_prs_miniPrs(
                                                                          "Обработать Mini PRS"))

        self.presission_button_horizon_mini.grid(row=1, column=0, padx=(10, 20), pady=(20, 0), sticky="nw")

        self.presission_button_horizon_union = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5,
                                                                       height=25, width=300, border_spacing=10,
                                                                       text='Объединить таблицы PRS и Mini PRS',
                                                                       fg_color=("#4040ff"), hover_color=("#4020e0"),
                                                                       command=lambda: self.read_and_preproc_prs_miniPrs(
                                                                           "Объединить таблицы PRS и Mini PRS"))
        self.presission_button_horizon_union.grid(row=2, column=0, padx=(10, 20), pady=(20, 0), sticky="nw")

        self.report_button_horizon = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5, height=25,
                                                             width=300, border_spacing=10,
                                                             text='Проверить значения (Prs/miniPrs)',
                                                             fg_color=("#4040ff"), hover_color=("#4020e0"),
                                                             command=self.call_check_range)
        self.report_button_horizon.grid(row=3, column=0, padx=(10, 20), pady=(20, 0), sticky="nw")

        self.export_button_horizon = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5, height=25,
                                                             width=300, border_spacing=10,
                                                             text='Экспорт в CSV', fg_color=("#4040ff"),
                                                             hover_color=("#4020e0"), command=self.export_results)
        self.export_button_horizon.grid(row=4, column=0, padx=(10, 20), pady=(20, 0), sticky="nw")

        self.export_rep_button_horizon = customtkinter.CTkButton(self.sidebar_horizon_frame, corner_radius=5, height=25,
                                                                 width=300, border_spacing=10,
                                                                 text='Очтет об ошибке (DOCX)', fg_color=("#4040ff"),
                                                                 hover_color=("#4020e0"), command=self.export_report)
        self.export_rep_button_horizon.grid(row=5, column=0, padx=(10, 20), pady=(20, 0), sticky="nw")

        self.progressbar_horizon = ttk.Progressbar(self.sidebar_horizon_frame, orient="horizontal",
                                                   length=200, mode="determinate")
        self.progressbar_horizon.grid(row=6, column=0, columnspan=1, padx=(10, 20),
                                      pady=(10, 0), sticky="nsew")

        # ------------------------------------------- CHEM ELEM ----------------------------------------------

        self.chem_elem_img = PhotoImage(file="./imgs/chem_elem.png")
        # Создаем вкладку "chem elem"
        self.chem_elem_frame = ttk.Frame(self.notebook)

        self.chem_elem_frame.grid_columnconfigure(1, weight=1)
        self.chem_elem_frame.grid_columnconfigure((2, 3), weight=0)
        self.chem_elem_frame.grid_rowconfigure((0, 1, 2, 3, 4, 6, 7, 8), weight=1)

        self.notebook.add(self.chem_elem_frame, text="Chem elem", image=self.chem_elem_img, compound=tk.LEFT)

        self.sidebar_chem_elem_frame = customtkinter.CTkFrame(self.chem_elem_frame, corner_radius=0)
        self.sidebar_chem_elem_frame.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_chem_elem_frame.grid_rowconfigure(8, weight=1)

        self.label_selected_files_chem_elem_mini = customtkinter.CTkLabel(master=self.sidebar_chem_elem_frame,
                                                                     text="Выбранные файлы (mini PRS):")
        self.label_selected_files_chem_elem_mini.grid(row=5, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.selected_files_text_chem_elem_mini = tk.Listbox(self.sidebar_chem_elem_frame, selectmode=tk.SINGLE,
                                                        bg='#FFFFFF', fg='#404040', height=30, width=50)
        self.selected_files_text_chem_elem_mini.grid(row=6, column=0, padx=20, sticky='nsew')
        self.selected_files_text_chem_elem_mini.bind("<Double-Button-1>", self.open_file_chem_elem)

        self.df_chem_elem = pd.DataFrame(self.dict_test)

        self.label_table_chem_elem = customtkinter.CTkLabel(master=self.chem_elem_frame, text="DataFrame:")
        self.label_table_chem_elem.grid(row=0, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_chem_elem = customtkinter.CTkFrame(self.chem_elem_frame, corner_radius=0)
        self.frame_table_chem_elem.grid(row=1, column=1, padx=(10, 10), sticky="nsew")

        self.table_chem_elem = Table(self.frame_table_chem_elem, dataframe=self.df_chem_elem, showtoolbar=True,
                                     showstatusbar=True)
        self.table_chem_elem.show()
        self.table_chem_elem.redraw()

        self.label_report_chem_elem = customtkinter.CTkLabel(master=self.chem_elem_frame, text="Отчет об ошибках:")
        self.label_report_chem_elem.grid(row=2, column=1, columnspan=1, ipadx=10, pady=(10, 0), sticky="nw")

        self.file_report_text_chem_elem = customtkinter.CTkTextbox(self.chem_elem_frame)
        self.file_report_text_chem_elem.grid(row=3, column=1, padx=(10, 20), pady=(0, 20), sticky="nsew")

        self.presission_button_chem_elem = customtkinter.CTkButton(self.sidebar_chem_elem_frame, corner_radius=5,
                                                                   height=25, width=300, border_spacing=10,
                                                                   text='Обработать Mini PRS',
                                                                   fg_color=("#00a0ff"), hover_color=("#0080e0"),
                                                                   command=lambda: self.read_and_preproc_prs_miniPrs(
                                                                       "Обработать Mini PRS"))
        self.presission_button_chem_elem.grid(row=0, column=0, padx=(10, 20), pady=(50, 20), sticky="nw")

        self.report_button_chem_elem = customtkinter.CTkButton(self.sidebar_chem_elem_frame, corner_radius=5, height=25,
                                                               width=300, border_spacing=10,
                                                               text='Проверить значения miniPrs',
                                                               fg_color=("#00a0ff"), hover_color=("#0080e0"),
                                                               command=self.call_check_range)
        self.report_button_chem_elem.grid(row=1, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.export_button_chem_elem = customtkinter.CTkButton(self.sidebar_chem_elem_frame, corner_radius=5, height=25,
                                                               width=300, border_spacing=10,
                                                               text='Экспорт в CSV', fg_color=("#00a0ff"),
                                                               hover_color=("#0080e0"), command=self.export_results)
        self.export_button_chem_elem.grid(row=2, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.export_rep_button_chem_elem = customtkinter.CTkButton(self.sidebar_chem_elem_frame, corner_radius=5,
                                                                   height=25, width=300, border_spacing=10,
                                                                   text='Очтет об ошибке (DOCX)', fg_color=("#00a0ff"),
                                                                   hover_color=("#0080e0"), command=self.export_report)
        self.export_rep_button_chem_elem.grid(row=3, column=0, padx=(10, 20), pady=(20, 20), sticky="nw")

        self.progressbar_chem_elem = ttk.Progressbar(self.sidebar_chem_elem_frame, orient="horizontal",
                                                   length=200, mode="determinate")
        self.progressbar_chem_elem.grid(row=4, column=0, columnspan=1, padx=(10, 20),
                                      pady=(10, 0), sticky="nsew")

        # ------------------------------------------- Загрузка -------------------------------------------------------
        self.uploading_img = PhotoImage(file='./imgs/upload.png')

        self.uploading_frame = ttk.Frame(self.notebook)

        self.uploading_frame.grid_columnconfigure(1, weight=1)
        self.uploading_frame.grid_columnconfigure((2, 3), weight=0)
        self.uploading_frame.grid_rowconfigure((0, 1, 2, 3, 4, 5), weight=1)

        self.notebook.add(self.uploading_frame, text="Загрузка данных в БД", image=self.uploading_img, compound=tk.LEFT)

        self.sidebar_uploading = customtkinter.CTkFrame(self.uploading_frame, corner_radius=0)
        self.sidebar_uploading.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_uploading.grid_rowconfigure(7, weight=1)

        self.label_selected_tables_uploading = customtkinter.CTkLabel(master=self.sidebar_uploading,
                                                                      text="Загрузить все таблицы В БД:")
        self.label_selected_tables_uploading.grid(row=0, column=0, columnspan=1, padx=20, pady=10, sticky="nw")
        # ///////////////////////таблица station
        self.label_table_uploading_station = customtkinter.CTkLabel(master=self.uploading_frame,
                                                                    text="DataFrame Station:")
        self.label_table_uploading_station.grid(row=0, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_uploading_station = customtkinter.CTkFrame(self.uploading_frame, corner_radius=0)
        self.frame_table_uploading_station.grid(row=1, column=1, padx=(10, 10), sticky="nsew")

        self.table_uploading_station = Table(self.frame_table_uploading_station, dataframe=self.df, showtoolbar=True,
                                             showstatusbar=True)
        self.table_uploading_station.show()
        self.table_uploading_station.redraw()
        # ///////////////////////таблица horizon
        self.label_table_uploading_horizon = customtkinter.CTkLabel(master=self.uploading_frame,
                                                                    text="DataFrame Horizon:")
        self.label_table_uploading_horizon.grid(row=2, column=1, columnspan=1, padx=20, sticky="nw")

        self.frame_table_uploading_horizon = customtkinter.CTkFrame(self.uploading_frame, corner_radius=0)
        self.frame_table_uploading_horizon.grid(row=3, column=1, padx=(10, 10), sticky="nsew")

        self.table_uploading_horizon = Table(self.frame_table_uploading_horizon, dataframe=self.df, showtoolbar=True,
                                             showstatusbar=True)
        self.table_uploading_horizon.show()
        self.table_uploading_horizon.redraw()
        # ///////////////////////таблица chem elem
        self.label_table_uploading_chem_elem = customtkinter.CTkLabel(master=self.uploading_frame,
                                                                      text="DataFrame Chem elem:")
        self.label_table_uploading_chem_elem.grid(row=4, column=1, columnspan=1, padx=20, sticky="nw")

        self.frame_table_uploading_chem_elem = customtkinter.CTkFrame(self.uploading_frame, corner_radius=0)
        self.frame_table_uploading_chem_elem.grid(row=5, column=1, padx=(10, 10), sticky="nsew")

        self.table_uploading_chem_elem = Table(self.frame_table_uploading_chem_elem, dataframe=self.df,
                                               showtoolbar=True, showstatusbar=True)
        self.table_uploading_chem_elem.show()
        self.table_uploading_chem_elem.redraw()
        # /////////////////////// загрузка всех данных
        self.button_uploading_datas = customtkinter.CTkButton(self.sidebar_uploading, corner_radius=5, height=25,
                                                              width=300, border_spacing=10, state="disabled",
                                                              text='Загрузить все таблицы в БД', fg_color=("#00a0ff"),
                                                              hover_color=("#0080e0"), command=self.uploading_datas_all)
        self.button_uploading_datas.grid(row=1, column=0, padx=(10, 20), pady=(50, 20), sticky="nw")

        # ----------------------------- Выгрузка и предпросмотр порейсово ----------------------------------------------------------------------
        self.downloading_img = PhotoImage(file='./imgs/ship.png')
        self.downloading_frame = ttk.Frame(self.notebook)
        self.downloading_frame.grid_columnconfigure(1, weight=1)
        self.downloading_frame.grid_columnconfigure((2, 3), weight=0)
        self.downloading_frame.grid_rowconfigure((0, 1, 2, 3, 4, 5), weight=1)

        self.notebook.add(self.downloading_frame,
                          text='Порейсовая выгрузка данных',
                          image=self.downloading_img,
                          compound=tk.LEFT)

        self.sidebar_downloading = customtkinter.CTkFrame(self.downloading_frame,
                                                          corner_radius=0)
        self.sidebar_downloading.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_downloading.grid_rowconfigure(20, weight=1)

        self.ship_label_down = customtkinter.CTkLabel(self.sidebar_downloading,
                                                      text="Введите позывные рейса (через запятую):")
        self.ship_label_down.grid(row=0, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.ship_entry_down = customtkinter.CTkEntry(self.sidebar_downloading)
        self.ship_entry_down.grid(row=1, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.reis_label_down = customtkinter.CTkLabel(self.sidebar_downloading,
                                                      text="Введите номера рейсов (через запятую):")
        self.reis_label_down.grid(row=2, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.reis_entry_down = customtkinter.CTkEntry(self.sidebar_downloading)
        self.reis_entry_down.grid(row=3, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.reis_label_preload = customtkinter.CTkLabel(self.sidebar_downloading,
                                                      text="Предпросмотр данных")
        self.reis_label_preload.grid(row=4, column=0, columnspan=1, padx=20, pady=(20, 10), sticky="nw")

        self.button_preload = customtkinter.CTkButton(self.sidebar_downloading,
                                                      text="Предпросмотр данных из Station",
                                                      corner_radius=5,
                                                      command=self.run_execute_query_station)
        self.button_preload.grid(row=5, column=0, sticky="nsew", padx=(10, 20), pady=(10, 5))

        self.button_preload_chem_elem_horizon = customtkinter.CTkButton(self.sidebar_downloading,
                                                                text="Предпросмоотр данных из Chem elem + Horizon",
                                                                corner_radius=5,
                                                                command=self.run_execute_query_chem_elem_horizon)
        self.button_preload_chem_elem_horizon.grid(row=6, column=0, sticky="nsew", padx=(10, 20), pady=(20, 5))

        self.reis_label_get = customtkinter.CTkLabel(self.sidebar_downloading,
                                                      text="Выгрузка данных")
        self.reis_label_get.grid(row=7, column=0, columnspan=1, padx=20, pady=(20, 10), sticky="nw")

        self.button_download_station = customtkinter.CTkButton(self.sidebar_downloading,
                                                               text="Выгрузить Station в CSV",
                                                               corner_radius=5,
                                                               command=self.get_columns_station)
        self.button_download_station.grid(row=8, column=0, sticky="nsew", padx=(10, 20), pady=(10, 5))

        self.button_download_horiz_chem = customtkinter.CTkButton(self.sidebar_downloading,
                                                                  text="Выгрузить Chem elem + Horizon в CSV",
                                                                  corner_radius=5,
                                                                  command=self.get_columns_horiz_chem)
        self.button_download_horiz_chem.grid(row=9, column=0, sticky="nsew", padx=(10, 20), pady=(15, 5))

        self.var_t = tk.BooleanVar()
        self.var_sl = tk.BooleanVar()
        self.var_o2 = tk.BooleanVar()
        self.var_po4 = tk.BooleanVar()
        self.var_sio3 = tk.BooleanVar()
        self.var_no2 = tk.BooleanVar()
        self.var_no3 = tk.BooleanVar()
        self.var_ph = tk.BooleanVar()
        self.var_no = tk.BooleanVar()
        self.var_bpk = tk.BooleanVar()
        self.var_nh4 = tk.BooleanVar()
        self.var_po = tk.BooleanVar()
        self.var_turb = tk.BooleanVar()

        # создаем Checkbutton для выбора столбца 't'
        self.cb_t = tk.Checkbutton(self.sidebar_downloading, text='T', variable=self.var_t)
        self.cb_t.grid(row=10, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'sl'
        self.cb_sl = tk.Checkbutton(self.sidebar_downloading, text='Sl', variable=self.var_sl)
        self.cb_sl.grid(row=11, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'o2'
        self.cb_o2 = tk.Checkbutton(self.sidebar_downloading, text='O2', variable=self.var_o2)
        self.cb_o2.grid(row=12, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Po4'
        self.cb_po4 = tk.Checkbutton(self.sidebar_downloading, text='Po4', variable=self.var_po4)
        self.cb_po4.grid(row=13, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Sio3'
        self.cb_sio3 = tk.Checkbutton(self.sidebar_downloading, text='Sio3', variable=self.var_sio3)
        self.cb_sio3.grid(row=14, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'No2'
        self.cb_no2 = tk.Checkbutton(self.sidebar_downloading, text='No2', variable=self.var_no2)
        self.cb_no2.grid(row=15, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'o2'
        self.cb_no3 = tk.Checkbutton(self.sidebar_downloading, text='No3', variable=self.var_no3)
        self.cb_no3.grid(row=16, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Ph'
        self.cb_ph = tk.Checkbutton(self.sidebar_downloading, text='Ph', variable=self.var_ph)
        self.cb_ph.grid(row=10, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'No'
        self.cb_no = tk.Checkbutton(self.sidebar_downloading, text='No', variable=self.var_no)
        self.cb_no.grid(row=11, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Bpk'
        self.cb_bpk = tk.Checkbutton(self.sidebar_downloading, text='Bpk', variable=self.var_bpk)
        self.cb_bpk.grid(row=12, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Nh4'
        self.cb_nh4 = tk.Checkbutton(self.sidebar_downloading, text='Nh4', variable=self.var_nh4)
        self.cb_nh4.grid(row=13, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Po'
        self.cb_po = tk.Checkbutton(self.sidebar_downloading, text='Po', variable=self.var_po)
        self.cb_po.grid(row=14, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Turb'
        self.cb_turb = tk.Checkbutton(self.sidebar_downloading, text='Turb', variable=self.var_turb)
        self.cb_turb.grid(row=15, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # /////////////////////// Таблица station
        self.label_table_down = customtkinter.CTkLabel(self.downloading_frame, text="Таблица Station")
        self.label_table_down.grid(row=0, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_down_station = customtkinter.CTkFrame(self.downloading_frame,
                                                               corner_radius=0)
        self.frame_table_down_station.grid(row=1, column=1, padx=(10, 10), sticky="nsew")

        self.table_down_station = Table(self.frame_table_down_station,
                                        dataframe=self.df,
                                        showtoolbar=True,
                                        showstatusbar=True)
        self.table_down_station.show()
        self.table_down_station.redraw()

        # /////////////////////// Таблицы horizon + chem_elem
        self.label_table_down_chem_elem = customtkinter.CTkLabel(self.downloading_frame,
                                                                 text="Таблица Chem_elem + Horizon")
        self.label_table_down_chem_elem.grid(row=4, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_down_chem_elem = customtkinter.CTkFrame(self.downloading_frame, corner_radius=0)
        self.frame_table_down_chem_elem.grid(row=5, column=1, padx=(10, 10), sticky="nsew")

        self.table_down_chem_elem = Table(self.frame_table_down_chem_elem, dataframe=self.df, showtoolbar=True,
                                          showstatusbar=True)
        self.table_down_chem_elem.show()
        self.table_down_chem_elem.redraw()

        # ----------------------------- Выгрузка и предпросмотр по РАЗРЕЗАМ --------------------------------------------
        self.downloading_img_rzr = PhotoImage(file='./imgs/section.png')
        self.downloading_frame_rzr = ttk.Frame(self.notebook)
        self.downloading_frame_rzr.grid_columnconfigure(1, weight=1)
        self.downloading_frame_rzr.grid_columnconfigure((2, 3), weight=0)
        self.downloading_frame_rzr.grid_rowconfigure((0, 1, 2, 3, 4, 5), weight=1)

        self.notebook.add(self.downloading_frame_rzr,
                          text='Выгрузка данных по разрезам',
                          image=self.downloading_img_rzr,
                          compound=tk.LEFT)

        self.sidebar_downloading_rzr = customtkinter.CTkFrame(self.downloading_frame_rzr,
                                                              corner_radius=0)
        self.sidebar_downloading_rzr.grid(row=0, column=0, rowspan=10, sticky="nsew")
        self.sidebar_downloading_rzr.grid_rowconfigure(20, weight=1)

        self.rzr1_label_down = customtkinter.CTkLabel(self.sidebar_downloading_rzr,
                                                      text="Введите разрез 1 (через запятую):")
        self.rzr1_label_down.grid(row=0, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        self.rzr1_entry_down = customtkinter.CTkEntry(self.sidebar_downloading_rzr)
        self.rzr1_entry_down.grid(row=1, column=0, columnspan=1, padx=20, pady=10, sticky="nw")

        # предпросмотр данных
        self.rzr_label_preload = customtkinter.CTkLabel(self.sidebar_downloading_rzr,
                                                      text="Предпросмотр данных")
        self.rzr_label_preload.grid(row=2, column=0, columnspan=1, padx=20, pady=(20, 5), sticky="nw")

        self.button_preload_rzr_station = customtkinter.CTkButton(self.sidebar_downloading_rzr,
                                                      text="Предпросмотр данных из Station",
                                                      corner_radius=5, command=self.run_execute_query_rzr_station)
        self.button_preload_rzr_station.grid(row=3, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        self.button_preload_rzr_horizon_chem = customtkinter.CTkButton(self.sidebar_downloading_rzr,
                                                      text="Предпросмотр данных из Horizon + Chem_elem",
                                                      corner_radius=5,
                                                      command=self.run_execute_query_rzr_horizon)
        self.button_preload_rzr_horizon_chem.grid(row=4, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # выгрузка данных
        self.rzr_label_get = customtkinter.CTkLabel(self.sidebar_downloading_rzr,
                                                      text="Выгрузка данных в CSV")
        self.rzr_label_get.grid(row=5, column=0, columnspan=1, padx=20, pady=(20, 5), sticky="nw")

        self.button_download_rzr_station = customtkinter.CTkButton(self.sidebar_downloading_rzr,
                                                       text="Выгрузить Station в CSV",
                                                       corner_radius=5, command=self.get_station_rzr)
        self.button_download_rzr_station.grid(row=6, column=0, sticky="nsew", padx=(10, 20), pady=(15, 5))

        self.button_download_rzr_horizon = customtkinter.CTkButton(self.sidebar_downloading_rzr,
                                                       text="Выгрузить Horizon + Chem elem в CSV",
                                                       corner_radius=5, command=self.get_horizon_rzr)
        self.button_download_rzr_horizon.grid(row=7, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        self.var_t_rzr = tk.BooleanVar()
        self.var_sl_rzr = tk.BooleanVar()
        self.var_o2_rzr = tk.BooleanVar()
        self.var_po4_rzr = tk.BooleanVar()
        self.var_sio3_rzr = tk.BooleanVar()
        self.var_no2_rzr = tk.BooleanVar()
        self.var_no3_rzr = tk.BooleanVar()
        self.var_ph_rzr= tk.BooleanVar()
        self.var_no_rzr = tk.BooleanVar()
        self.var_bpk_rzr = tk.BooleanVar()
        self.var_nh4_rzr = tk.BooleanVar()
        self.var_po_rzr = tk.BooleanVar()
        self.var_turb_rzr = tk.BooleanVar()

        # создаем Checkbutton для выбора столбца 't'
        self.cb_t_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='T', variable=self.var_t_rzr)
        self.cb_t_rzr.grid(row=8, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'sl'
        self.cb_sl_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Sl', variable=self.var_sl_rzr)
        self.cb_sl_rzr.grid(row=9, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'o2'
        self.cb_o2_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='O2', variable=self.var_o2_rzr)
        self.cb_o2_rzr.grid(row=10, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Po4'
        self.cb_po4_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Po4', variable=self.var_po4_rzr)
        self.cb_po4_rzr.grid(row=11, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Sio3'
        self.cb_sio3_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Sio3', variable=self.var_sio3_rzr)
        self.cb_sio3_rzr.grid(row=12, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'No2'
        self.cb_no2_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='No2', variable=self.var_no2_rzr)
        self.cb_no2_rzr.grid(row=13, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'o2'
        self.cb_no3_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='No3', variable=self.var_no3_rzr)
        self.cb_no3_rzr.grid(row=14, column=0, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Ph'
        self.cb_ph_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Ph', variable=self.var_ph_rzr)
        self.cb_ph_rzr.grid(row=8, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'No'
        self.cb_no_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='No', variable=self.var_no_rzr)
        self.cb_no_rzr.grid(row=9, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Bpk'
        self.cb_bpk_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Bpk', variable=self.var_bpk_rzr)
        self.cb_bpk_rzr.grid(row=10, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Nh4'
        self.cb_nh4_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Nh4', variable=self.var_nh4_rzr)
        self.cb_nh4_rzr.grid(row=11, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Po'
        self.cb_po_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Po', variable=self.var_po_rzr)
        self.cb_po_rzr.grid(row=12, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # создаем Checkbutton для выбора столбца 'Turb'
        self.cb_turb_rzr = tk.Checkbutton(self.sidebar_downloading_rzr, text='Turb', variable=self.var_turb_rzr)
        self.cb_turb_rzr.grid(row=13, column=1, sticky="nsew", padx=(10, 20), pady=(25, 5))

        # ///////////////////////таблица station
        self.label_table_down_rzr = customtkinter.CTkLabel(self.downloading_frame_rzr,
                                                           text="Таблица Station")
        self.label_table_down_rzr.grid(row=0, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_down_station_rzr = customtkinter.CTkFrame(self.downloading_frame_rzr,
                                                                   corner_radius=0)
        self.frame_table_down_station_rzr.grid(row=1, column=1, padx=(10, 10), sticky="nsew")

        self.table_down_station_rzr = Table(self.frame_table_down_station_rzr,
                                            dataframe=self.df,
                                            showtoolbar=True,
                                            showstatusbar=True)
        self.table_down_station_rzr.show()
        self.table_down_station_rzr.redraw()

        # ///////////////////////таблица horizon
        self.label_table_down_horizon_chem_rzr = customtkinter.CTkLabel(self.downloading_frame_rzr,
                                                                        text="Таблица Horizon + Chem_elem")
        self.label_table_down_horizon_chem_rzr.grid(row=2, column=1, columnspan=1, padx=10, sticky="nw")

        self.frame_table_down_horizon_rzr = customtkinter.CTkFrame(self.downloading_frame_rzr,
                                                                   corner_radius=0)
        self.frame_table_down_horizon_rzr.grid(row=3, column=1, padx=(10, 10), sticky="nsew")

        self.table_down_horizon_chem_rzr = Table(self.frame_table_down_horizon_rzr,
                                            dataframe=self.df,
                                            showtoolbar=True,
                                            showstatusbar=True)
        self.table_down_horizon_chem_rzr.show()
        self.table_down_horizon_chem_rzr.redraw()
        # --------- // ----------
        # Показываем окно
        self.notebook.pack(fill="both", expand=True)

    # ВЫЗОВ ФУНКЦИЙ:
    # ============== Выгрзука и предпросмотр по РАЗРЕЗАМ ==============
    # предпросмотр таблицы station
    def run_execute_query_rzr_station(self):
        current_rzr1 = self.rzr1_entry_down.get().upper()
        DatabaseUploader.preload_datas_rzrs_station(self, current_rzr1)

    # предпросмотр таблицы horizon
    def run_execute_query_rzr_horizon(self):
        current_rzr1 = self.rzr1_entry_down.get().upper()
        columns = ['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                   'longitude', 'Depth', 'Horizon']
        if self.var_t_rzr.get():
            columns.append('T')
        if self.var_sl_rzr.get():
            columns.append('Sl')
        if self.var_o2_rzr.get():
            columns.append('O2')
        if self.var_po4_rzr.get():
            columns.append('Po4')
        if self.var_sio3_rzr.get():
            columns.append('Sio3')
        if self.var_no2_rzr.get():
            columns.append('No2')
        if self.var_no3_rzr.get():
            columns.append('No3')
        if self.var_ph_rzr.get():
            columns.append('Ph')
        if self.var_no_rzr.get():
            columns.append('No')
        if self.var_bpk_rzr.get():
            columns.append('Bpk')
        if self.var_nh4_rzr.get():
            columns.append('Nh4')
        if self.var_po_rzr.get():
            columns.append('Po')
        if self.var_turb_rzr.get():
            columns.append('Turb')
        if columns:
            DatabaseUploader.preload_datas_rzrs_horizon(self, current_rzr1, columns)

    # выгрузка данных из таблицы station
    def get_station_rzr(self):
        DatabaseUploader.get_station_rzr(self)

    # выгрузка данных из таблицы station
    def get_horizon_rzr(self):
        DatabaseUploader.get_horizon_rzr(self)
    # ============== // Выгрзука и предпросмотр по РАЗРЕЗАМ ==============

    # ============== Порейсовые предпросмотр и выгрузка ==============
    # предпросмотр Station
    def run_execute_query_station(self):
        self.current_ship = self.ship_entry_down.get().upper()
        self.current_reis = self.reis_entry_down.get()
        DatabaseUploader.preload_datas_reis_station(self, self.current_ship, self.current_reis)

    # предпросмотр Chem_elem + Horizon
    def run_execute_query_chem_elem_horizon(self):
        self.current_ship = self.ship_entry_down.get().upper()
        self.current_reis = self.reis_entry_down.get()
        columns = ['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                   'longitude', 'Depth', 'Horizon']
        if self.var_t.get():
            columns.append('T')
        if self.var_sl.get():
            columns.append('Sl')
        if self.var_o2.get():
            columns.append('O2')
        if self.var_po4.get():
            columns.append('Po4')
        if self.var_sio3.get():
            columns.append('Sio3')
        if self.var_no2.get():
            columns.append('No2')
        if self.var_no3.get():
            columns.append('No3')
        if self.var_ph.get():
            columns.append('Ph')
        if self.var_no.get():
            columns.append('No')
        if self.var_bpk.get():
            columns.append('Bpk')
        if self.var_nh4.get():
            columns.append('Nh4')
        if self.var_po.get():
            columns.append('Po')
        if self.var_turb.get():
            columns.append('Turb')
        if columns:
            DatabaseUploader.preload_datas_reis_chem_elem_horizon(self, self.current_ship, self.current_reis, columns)

    # выгрузка данных из station в CSV
    def get_columns_station(self):
        DatabaseUploader.down_reis_station(self)

    # выгрузка данных из chem_elem + horizon в CSV
    def get_columns_horiz_chem(self):
        columns = ['Ship', 'Reis', 'Station', 'Data', 'Time', 'latitude',
                   'longitude', 'Depth', 'Horizon']
        if self.var_t.get():
            columns.append('T')
        if self.var_sl.get():
            columns.append('Sl')
        if self.var_o2.get():
            columns.append('O2')
        if self.var_po4.get():
            columns.append('Po4')
        if self.var_sio3.get():
            columns.append('Sio3')
        if self.var_no2.get():
            columns.append('No2')
        if self.var_no3.get():
            columns.append('No3')
        if self.var_ph.get():
            columns.append('Ph')
        if self.var_no.get():
            columns.append('No')
        if self.var_bpk.get():
            columns.append('Bpk')
        if self.var_nh4.get():
            columns.append('Nh4')
        if self.var_po.get():
            columns.append('Po')
        if self.var_turb.get():
            columns.append('Turb')
        if columns:
            DatabaseUploader.get_reis_horiz_chem(self, columns)
    # ============== // Порейсовые предпросмотр и выгрузка ==============

    # выбор файлов prs-файлов
    def select_folder_prs(self):
        self.txt_files = filedialog.askopenfilenames(title="Select file",
                                                     filetypes=(("Text files", "*.prs"), ("all files", "*.*")))

        self.selected_files_text.delete(0, tk.END)
        self.selected_files_text_horizon.delete(0, tk.END)
        for file in self.txt_files:
            if file.endswith(".prs"):
                self.selected_files_text.insert(tk.END, file)
                self.selected_files_text_horizon.insert(tk.END, file)

    # выбор файлов mini_prs-файлов
    def select_folder_mini(self):
        self.txt_files_mini = filedialog.askopenfilenames(title="Select file",
                                                     filetypes=(("Text files", "*.prs"), ("all files", "*.*")))

        self.selected_files_text_horizon_mini.delete(0, tk.END)
        self.selected_files_text_chem_elem_mini.delete(0, tk.END)
        for file in self.txt_files_mini:
            if file.endswith(".prs"):
                self.selected_files_text_horizon_mini.insert(tk.END, file)
                self.selected_files_text_chem_elem_mini.insert(tk.END, file)

    # открытие файлов при нажатии на них
    def open_file(self, event):
        selected_file = self.selected_files_text.get(self.selected_files_text.curselection())
        subprocess.run(["notepad.exe", selected_file])

    # открытие файлов при нажатии на них
    def open_file_horizon(self, event):
        selected_file_horizon = self.selected_files_text_horizon.get(
            self.selected_files_text_horizon.curselection())
        subprocess.run(["notepad.exe", selected_file_horizon])
    # открытие файлов при нажатии на них

    def open_file_horizon_mini(self, event):
        selected_file_horizon = self.selected_files_text_horizon_mini.get(
            self.selected_files_text_horizon_mini.curselection())
        subprocess.run(["notepad.exe", selected_file_horizon])

    # открытие файлов при нажатии на них
    def open_file_chem_elem(self, event):
        selected_file_chem_elem = self.selected_files_text_chem_elem_mini.get(
            self.selected_files_text_chem_elem_mini.curselection())
        subprocess.run(["notepad.exe", selected_file_chem_elem])

    # =============================== station ===============================
    # обработка PRS в таблицу station
    def read_files(self):
        try:
            self.progressbar_station.start(10)
            thread = threading.Thread(target=FileProcessor.process_files, args=(self, self.txt_files))
            thread.start()
            def check_thread():
                if thread.is_alive():
                    self.sidebar_frame.after(100, check_thread)
                else:
                    self.progressbar_station.stop()
            self.sidebar_frame.after(100, check_thread)

        except Exception as e:
            self.file_report_text.delete("1.0", tk.END)
            self.file_report_text.insert(tk.END, f"Не сработало! {e}")

    # проверка значений на ошибки
    def call_check_range(self):
        self.current_note = self.notebook.tab("current", "text")
        CheckDatas.call_check_range(self, self.current_note)

    # экспорт обработанных PRS в CSV
    def export_results(self):
        self.current_note = self.notebook.tab("current", "text")
        ExportingFiles.export_results(self, self.current_note)

    # экспорт ошибок в Word
    def export_report(self):
        self.current_note = self.notebook.tab("current", "text")
        ExportingFiles.export_report(self, self.current_note)

    def delete_reis(self):
        self.current_note = self.notebook.tab("current", "text")
        DatabaseUploader.delete_data(self, self.current_note, self.ship.get(), self.reis.get())

    # =============================== Обработка PRS в таблицы horizon, chem_elem ===============================
    def read_and_preproc_prs_miniPrs(self, btn_text):
        text_btn = btn_text
        self.current_note = self.notebook.tab("current", "text")
        if text_btn == "Обработать Mini PRS":
            if self.current_note == "Chem elem":
                self.progressbar_chem_elem.start(10)
                thread = threading.Thread(target=PrsProcessor.prs_read_and_prepocessing,
                                      args=(self, self.txt_files_mini, self.current_note, text_btn))
                thread.start()
                def check_thread():
                    if thread.is_alive():
                        self.sidebar_chem_elem_frame.after(100, check_thread)
                    else:
                        self.progressbar_chem_elem.stop()
                self.sidebar_chem_elem_frame.after(100, check_thread)

            elif self.current_note == "Horizon":
                self.progressbar_horizon.start(10)
                thread = threading.Thread(target=PrsProcessor.prs_read_and_prepocessing,
                                      args=(self, self.txt_files_mini, self.current_note, text_btn))
                thread.start()
                def check_thread():
                    if thread.is_alive():
                        self.sidebar_horizon_frame.after(100, check_thread)
                    else:
                        self.progressbar_horizon.stop()
                self.sidebar_horizon_frame.after(100, check_thread)

        elif text_btn == "Обработать PRS":
            self.progressbar_horizon.start(10)
            thread = threading.Thread(target=PrsProcessor.prs_read_and_prepocessing,
                                  args=(self, self.txt_files, self.current_note, text_btn))
            thread.start()
            def check_thread():
                if thread.is_alive():
                    self.sidebar_horizon_frame.after(100, check_thread)
                else:
                    self.progressbar_horizon.stop()
            self.sidebar_horizon_frame.after(100, check_thread)

        elif text_btn == "Объединить таблицы PRS и Mini PRS":
            self.progressbar_horizon.start(10)
            thread = threading.Thread(target=PrsProcessor.prs_read_and_prepocessing,
                                  args=(self, self.txt_files, self.current_note, text_btn))
            thread.start()
            def check_thread():
                if thread.is_alive():
                    self.sidebar_horizon_frame.after(100, check_thread)
                else:
                    self.progressbar_horizon.stop()
            self.sidebar_horizon_frame.after(100, check_thread)

    # =============================== Загрузка всех таблиц в БД ===============================
    def uploading_datas_all(self):
        self.current_note = self.notebook.tab("current", "text")
        DatabaseUploader.upload_all_tables(self, self.current_note)


if __name__ == "__main__":
    root = customtkinter.CTk()
    app = App(root)
    root.mainloop()
