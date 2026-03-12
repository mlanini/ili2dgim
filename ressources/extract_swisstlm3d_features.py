import re
import csv

# Percorso del file .ili da analizzare
ili_file = 'ressources/swissTLM3D_ili2_V2_3.ili'
# Percorso del file CSV di output
csv_file = 'swissTLM3D_features.csv'

# Regex per trovare le dichiarazioni di CLASS o STRUCTURE
pattern = re.compile(r'\b(CLASS|STRUCTURE)\s+(\w+)')

features = []

with open(ili_file, 'r', encoding='utf-8') as f:
    for line in f:
        match = pattern.search(line)
        if match:
            features.append(match.group(2))

# Scrivi le feature in un file CSV
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Feature_swissTLM3D'])
    for feature in features:
        writer.writerow([feature])

print(f"Trovate {len(features)} feature. Salvate in {csv_file}")
