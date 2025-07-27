import pandas as pd
import os

# Ścieżka do pliku .xls (ale tak naprawdę CSV)
xls_file_path = r"C:\Users\LEN\Desktop\NBA Project\NBA Data\NBA_Player_stats.2425.csv"
csv_output_path = r"C:\Users\LEN\Desktop\NBA Project\NBA Data\NBA_Player_Stats_2425.csv"

# Próba odczytu z kodowaniem windows-1250
df = pd.read_csv(xls_file_path, encoding='windows-1250', sep=';', engine='python')

# Zapis do pliku CSV
df.to_csv(csv_output_path, index=False, encoding='utf-8')
print("✅ Zapisano do pliku:", csv_output_path)
