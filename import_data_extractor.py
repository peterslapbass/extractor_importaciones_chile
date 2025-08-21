import pandas as pd
import tkinter as tk
from tkinter import ttk
import traceback
import tkinter.font as tkFont
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import numpy as np

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Función para cargar la descripción de datos
def cargar_descripcion(file_path, sheet_name):
    df2 = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=1)
    df2 = df2.drop(columns=['Unnamed: 3', 'Datos a publicar Nuevo formato'])

    # Mapear los tipos de datos
    type_mapping = {
        'NUMBER': 'float64',
        'VARCHAR2': 'object',
        'DATE': 'datetime64[ns]'
    }
    df2['tipo'] = df2['tipo'].map(type_mapping)

    return df2

# Variable global para controlar si el proceso debe continuar
keep_running = True

# Lista global para almacenar los valores únicos de ARANC_NAC
unique_aranc_nac_set = set()


# DESCRIPCION PATH
descripcion_filepath = r'descripcion-y-estructura-de-datos.xlsx'
descripcion_sheet_name = 'DIN'

# Cargar la descripción de columnas una sola vez al inicio
descripcion_cache = cargar_descripcion(descripcion_filepath, descripcion_sheet_name)

# Función para leer el archivo CSV
def load_csv(file_path, chunksize=10000):
    try:
        # Leer el archivo en chunks y concatenarlos en un solo DataFrame
        chunks = pd.read_csv(file_path, sep=';', encoding='latin1', decimal=",", chunksize=chunksize)
        df = pd.concat(chunks)
        return df
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None

# Función para procesar el CSV y filtrar por términos de búsqueda usando la descripción cacheada
def process_csv(df, search_terms, descripcion_cache, search_by='ARANC_NAC'):
    try:
        df2 = descripcion_cache  # Se reutiliza la descripción cargada

        # Usar los valores de la columna 'CAMPO - DIN -  ENCABEZADO' como nombres de las columnas
        new_header = df2['CAMPO - DIN -  ENCABEZADO'].str.strip().values
        if len(df.columns) == len(new_header):
            df.columns = new_header
        else:
            print(f"Número de columnas no coincide en el archivo {descripcion_filepath}")
            return None, None
        
        df.columns = df.columns.str.replace(' ', '')

        # Extraer el nombre de la columna ya limpiado
        df2['campo'] = df2['CAMPO - DIN -  ENCABEZADO'].str.strip()

        # Procesar columnas de tipo objeto (texto) de forma vectorizada
        object_cols = df2[df2['tipo'] == 'object'][['campo', 'largo']]
        for col, max_len in zip(object_cols['campo'], object_cols['largo']): 
            df[col] = df[col].astype(str).str[:int(max_len)]

        # Procesar columnas numéricas de forma vectorizada
        float_cols = df2[df2['tipo'] == 'float64']['campo'].tolist()
        if float_cols:
            df[float_cols] = df[float_cols].apply(pd.to_numeric, errors='coerce')

        # Procesar columnas de tipo fecha de forma vectorizada
        date_cols = df2[df2['tipo'] == 'datetime64[ns]']['campo'].tolist()
        if date_cols:
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], format='%d%m%Y', errors='coerce')

        # Asegurarse de que la columna de búsqueda sea string
        if search_by in df.columns:
            df[search_by] = df[search_by].astype(str)
        else:
            print(f"La columna '{search_by}' no se encuentra en el DataFrame")
            return None, None

        # Filtrado según el tipo de búsqueda
        if search_by == 'NUM_UNICO_IMPORTADOR':
            mask = df[search_by].isin(search_terms)
        else:
            mask = df[search_by].str.startswith(tuple(search_terms), na=False)
        
        df_filtered = df[mask].copy()

        # Agregar la columna "search_term" de forma vectorizada:
        if search_by == 'NUM_UNICO_IMPORTADOR':
            df_filtered['search_term'] = df_filtered[search_by]
        else:
            conditions = [df_filtered[search_by].str.startswith(term, na=False) for term in search_terms]
            df_filtered['search_term'] = np.select(conditions, search_terms, default="")

        # Procesamiento adicional vectorizado
        if 'CIF_ITEM' in df_filtered.columns:
            df_filtered['CIF_ITEM'] = pd.to_numeric(df_filtered['CIF_ITEM'], errors='coerce')
        else:
            print("La columna 'CIF_ITEM' no se encuentra en el DataFrame")
            return None, None

        if 'DD' in df_filtered.columns:
            df_filtered['DD'] = pd.to_datetime(df_filtered['DD'], format='%d%m%Y', errors='coerce')
        else:
            print("La columna 'DD' no se encuentra en el DataFrame")
            return None, None
        return df_filtered, df_filtered
    except Exception as e:
        print(f"Error al procesar el CSV: {e}")
        print(traceback.format_exc())
        return None, None
# Función para procesar un archivo
def process_file(file_path, search_terms, descripcion_cache, search_by='ARANC_NAC'):
    df = load_csv(file_path)
    if df is not None:
        processed_df, original_df = process_csv(df, search_terms, descripcion_cache, search_by)
        return processed_df, original_df
    return None, None

# Función para escanear las carpetas y procesar los CSV en paralelo
def scan_and_process_folders(base_path, years, search_terms, status_label, search_by='ARANC_NAC', export_name=None):
    global keep_running
    total_files = sum([sum(1 for f in files if f.endswith('.txt') and 'processed' not in f) for r, d, files in os.walk(base_path)])
    processed_files = 0
    all_processed_data = []

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for year in years:
            year_path = os.path.join(base_path, str(year))
            for root, dirs, files in os.walk(year_path):
                for file in files:
                    if not keep_running:
                        print("Proceso detenido")
                        return
                    if file.endswith(".txt") and 'processed' not in file:
                        file_path = os.path.join(root, file)
                        futures.append(executor.submit(process_file, file_path, search_terms, descripcion_cache, search_by))

        for future in as_completed(futures):
            if not keep_running:
                print("Proceso detenido")
                return
            processed_df, original_df = future.result()
            if original_df is not None:
                all_processed_data.append(processed_df)
                processed_files += 1
                progress['value'] = (processed_files / total_files) * 100
                progress.update_idletasks()

    # Forzamos la barra a mostrar el 100% al finalizar
    progress['value'] = 100
    progress.update_idletasks()

    # Exportar todos los resultados del grupo juntos
    if all_processed_data:
        final_df = pd.concat(all_processed_data)
        export_to_excel(final_df, export_name, search_by)

    status_label.config(text="Procesamiento completado")

# Función para exportar los datos a un archivo CSV
def export_to_excel(df, export_name, search_by):
    try:
        # El nombre del archivo refleja los códigos combinados por guion
        if search_by == 'ARANC_NAC':
            output_path = f'output/Resultados_ARANC_NAC_{export_name}.csv'
        elif search_by == 'NUM_UNICO_IMPORTADOR':
            output_path = f'output/Resultados_NUM_UNICO_IMPORTADOR_{export_name}.csv'
        else:
            output_path = f'output/Resultados_{export_name}.csv'

        df.to_csv(output_path, index=False)
        print(f"Datos exportados a {output_path}")
    except Exception as e:
        print(f"Error al exportar a CSV: {e}")
        print(traceback.format_exc())

# Modificar la función para procesar términos de búsqueda
def start_search():
    global keep_running, years_range
    keep_running = True

    # Dividir términos de búsqueda por comas y guiones
    raw_terms = search_var.get()
    search_groups = [group.strip() for group in raw_terms.split(',')]
    
    search_by = search_by_var.get()

    # Obtener el rango de años desde la interfaz
    try:
        start_year = int(start_year_var.get())
        end_year = int(end_year_var.get())
        years_range = range(start_year, end_year + 1)
    except ValueError:
        status_label.config(text="Por favor, ingrese años válidos.")
        return

    status_label.config(text="Procesando...")
    
    descripcion = descripcion_var.get().strip().replace(' ', '_')

    # Procesar cada grupo de términos
    for group in search_groups:
        search_terms = [term.strip() for term in group.split('-')]
        export_name = group.replace('-', '_')
        # Agrega la descripción al nombre del archivo
        if descripcion:
            export_name = f"{export_name}_{descripcion}"
        search_thread = threading.Thread(
            target=scan_and_process_folders,
            args=(base_directory, years_range, search_terms, status_label, search_by, export_name)
        )
        search_thread.start()

# Función para detener la búsqueda
def stop_search():
    global keep_running
    keep_running = False

# Función para cerrar la aplicación y terminar el proceso
def on_closing():
    stop_search()
    root.destroy()
    sys.exit()

# Crear la ventana principal
root = tk.Tk()
root.title("Buscar en CSV")

# Crear un PanedWindow para dividir la ventana
paned_window = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
paned_window.pack(fill=tk.BOTH, expand=1)

# Frame para la búsqueda principal
main_frame = ttk.Frame(paned_window)
paned_window.add(main_frame, weight=3)

search_var = tk.StringVar()
search_by_var = tk.StringVar(value="ARANC_NAC")

search_label = tk.Label(main_frame, text="Buscar (separar términos con comas):")
search_label.pack(pady=5)
search_entry = tk.Entry(main_frame, textvariable=search_var)
search_entry.pack(pady=5)
search_button = tk.Button(main_frame, text="Buscar", command=start_search)
search_button.pack(pady=5)

stop_button = tk.Button(main_frame, text="Detener", command=stop_search)
stop_button.pack(pady=5)

# Agregar entradas para definir el rango de años de forma interactiva
start_year_var = tk.StringVar(value="2017")
end_year_var = tk.StringVar(value="2025")

start_year_label = tk.Label(main_frame, text="Año de inicio:")
start_year_label.pack(pady=5)
start_year_entry = tk.Entry(main_frame, textvariable=start_year_var, width=10)
start_year_entry.pack(pady=5)

end_year_label = tk.Label(main_frame, text="Año final:")
end_year_label.pack(pady=5)
end_year_entry = tk.Entry(main_frame, textvariable=end_year_var, width=10)
end_year_entry.pack(pady=5)

# Agregar opción para seleccionar el tipo de búsqueda
search_by_label = tk.Label(main_frame, text="Buscar por:")
search_by_label.pack(pady=5)
search_by_aranc_nac = tk.Radiobutton(main_frame, text="ARANC_NAC", variable=search_by_var, value="ARANC_NAC")
search_by_aranc_nac.pack(pady=5)
search_by_num_importador = tk.Radiobutton(main_frame, text="NUM_UNICO_IMPORTADOR", variable=search_by_var, value="NUM_UNICO_IMPORTADOR")
search_by_num_importador.pack(pady=5)

# Etiqueta de estado
status_label = tk.Label(main_frame, text="")
status_label.pack(pady=10)

error_label = tk.Label(main_frame, text="", fg="red")
error_label.pack(pady=5)

# Barra de progreso
progress = ttk.Progressbar(main_frame, orient='horizontal', length=400, mode='determinate')
progress.pack(pady=10)

# Parámetros de búsqueda
years_range = range(2017, 2025)
base_directory = os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Manejar el cierre de la ventana para terminar el proceso
root.protocol("WM_DELETE_WINDOW", on_closing)

descripcion_var = tk.StringVar()

descripcion_label = tk.Label(main_frame, text="Descripción para el archivo exportado:")
descripcion_label.pack(pady=5)
descripcion_entry = tk.Entry(main_frame, textvariable=descripcion_var)
descripcion_entry.pack(pady=5)

root.mainloop()

