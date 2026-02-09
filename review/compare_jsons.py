#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import re

f1_path = r'C:\Users\herlbeng\Documents\Platform Partners\platform_partners\etl_servicetitan\st2json-job\return_1.json'
f2_path = r'C:\Users\herlbeng\Documents\Platform Partners\platform_partners\etl_servicetitan\st2json-job\return_2.json'

print("="*80)
print("COMPARACIÓN DETALLADA DE JSONs")
print("="*80)

# Leer archivos
with open(f1_path, 'r', encoding='utf-8') as f:
    c1 = f.read()
    
with open(f2_path, 'r', encoding='utf-8') as f:
    c2 = f.read()

# Parsear JSONs
d1 = json.loads(c1)
d2 = json.loads(c2)

print("\n=== TAMAÑOS ===")
print(f"return_1: {len(c1):,} bytes ({len(c1)/1024/1024:.2f} MB)")
print(f"return_2: {len(c2):,} bytes ({len(c2)/1024/1024:.2f} MB)")
print(f"Diferencia: {abs(len(c1)-len(c2)):,} bytes")

print("\n=== ESTRUCTURA ===")
print(f"return_1: {len(d1['data'])} items")
print(f"return_2: {len(d2['data'])} items")

print("\n=== CARACTERES ESPECIALES EN STRINGS (ESCAPADOS) ===")
# Buscar \r\n escapados en strings
escaped_rn_1 = c1.count('\\r\\n')
escaped_r_1 = c1.count('\\r')
escaped_n_1 = c1.count('\\n')
escaped_t_1 = c1.count('\\t')

escaped_rn_2 = c2.count('\\r\\n')
escaped_r_2 = c2.count('\\r')
escaped_n_2 = c2.count('\\n')
escaped_t_2 = c2.count('\\t')

print(f"return_1: \\r\\n={escaped_rn_1}, \\r={escaped_r_1}, \\n={escaped_n_1}, \\t={escaped_t_1}")
print(f"return_2: \\r\\n={escaped_rn_2}, \\r={escaped_r_2}, \\n={escaped_n_2}, \\t={escaped_t_2}")

if escaped_rn_2 > 0:
    print(f"\nIMPORTANTE: return_2 tiene {escaped_rn_2} secuencias \\r\\n escapadas en strings!")
    print(f"return_1 tiene {escaped_rn_1} secuencias \\r\\n escapadas")
    # Encontrar ejemplos
    matches = re.findall(r'"[^"]*\\r\\n[^"]*"', c2)
    print(f"Encontrados {len(matches)} strings con \\r\\n, primeros 3:")
    for i, match in enumerate(matches[:3], 1):
        print(f"  {i}. {match[:100]}...")

print("\n=== BUSCANDO CARACTERES DE CONTROL REALES (NO ESCAPADOS) ===")
real_r_1 = c1.count('\r')
real_n_1 = c1.count('\n')
real_t_1 = c1.count('\t')

real_r_2 = c2.count('\r')
real_n_2 = c2.count('\n')
real_t_2 = c2.count('\t')

print(f"return_1: real \\r={real_r_1}, real \\n={real_n_1}, real \\t={real_t_1}")
print(f"return_2: real \\r={real_r_2}, real \\n={real_n_2}, real \\t={real_t_2}")

print("\n=== ANÁLISIS DE DESCRIPTION FIELDS ===")
# Buscar campos description con \r\n
descriptions_1 = []
descriptions_2 = []

for item in d1['data']:
    if 'items' in item:
        for item_detail in item['items']:
            if 'description' in item_detail and item_detail['description']:
                desc = item_detail['description']
                if '\r' in desc or '\n' in desc:
                    descriptions_1.append(desc)

for item in d2['data']:
    if 'items' in item:
        for item_detail in item['items']:
            if 'description' in item_detail and item_detail['description']:
                desc = item_detail['description']
                if '\r' in desc or '\n' in desc:
                    descriptions_2.append(desc)

print(f"return_1: {len(descriptions_1)} descriptions con \\r o \\n reales")
print(f"return_2: {len(descriptions_2)} descriptions con \\r o \\n reales")

if descriptions_2:
    print("\nPrimeros 3 ejemplos de descriptions en return_2 con \\r o \\n:")
    for i, desc in enumerate(descriptions_2[:3], 1):
        print(f"\n{i}. {repr(desc[:200])}")

# Buscar en el JSON crudo (escapado)
matches_rn_in_strings = re.findall(r':"([^"]*\\r\\n[^"]*)"', c2)
print(f"\n=== STRINGS CON \\r\\n ESCAPADOS EN JSON RAW ===")
print(f"return_2: {len(matches_rn_in_strings)} strings con \\r\\n escapado")
if matches_rn_in_strings:
    print("Primeros 3 ejemplos:")
    for i, match in enumerate(matches_rn_in_strings[:3], 1):
        print(f"\n{i}. {match[:150]}")

print("\n=== VALIDACION JSON ===")
try:
    json.loads(c1)
    print("OK return_1: JSON valido")
except json.JSONDecodeError as e:
    print(f"ERROR return_1: JSON invalido - {e}")

try:
    json.loads(c2)
    print("OK return_2: JSON valido")
except json.JSONDecodeError as e:
    print(f"ERROR return_2: JSON invalido - {e}")

print("\n=== ANALISIS DE ESTRUCTURA ===")
print(f"return_1 keys: {list(d1.keys())}")
print(f"return_2 keys: {list(d2.keys())}")

# Verificar balance de llaves y corchetes
def check_balance(text, pos=None):
    """Verifica balance de llaves y corchetes hasta una posición específica"""
    braces = 0
    brackets = 0
    limit = pos if pos else len(text)
    for i, char in enumerate(text[:limit]):
        if char == '{':
            braces += 1
        elif char == '}':
            braces -= 1
        elif char == '[':
            brackets += 1
        elif char == ']':
            brackets -= 1
    return braces, brackets

print("\n=== VERIFICACION DE BALANCE (simulando truncamiento) ===")
# Verificar balance en diferentes puntos
test_positions = [1000000, 2000000, 3000000, 4000000, 5000000, 6000000, len(c2)]
for pos in test_positions:
    if pos <= len(c2):
        braces, brackets = check_balance(c2, pos)
        print(f"En posición {pos:,}: braces={braces}, brackets={brackets}")

print("\n=== ANALISIS DE CARACTERES NO-ASCII ===")
nonascii_1 = set(c for c in c1 if ord(c) > 127)
nonascii_2 = set(c for c in c2 if ord(c) > 127)
print(f"return_1: {len(nonascii_1)} caracteres unicos no-ASCII")
print(f"return_2: {len(nonascii_2)} caracteres unicos no-ASCII")
if nonascii_2:
    print(f"return_2 tiene {len(nonascii_2)} caracteres Unicode unicos (no-ASCII)")

print("\n=== PRUEBA DE CODIFICACION (como lo haria requests) ===")
# Simular cómo requests procesaría el contenido
try:
    # Simular response.content (bytes)
    c1_bytes = c1.encode('utf-8')
    c2_bytes = c2.encode('utf-8')
    print(f"return_1 como bytes: {len(c1_bytes):,} bytes")
    print(f"return_2 como bytes: {len(c2_bytes):,} bytes")
    
    # Intentar decodificar y parsear como lo haría requests
    c1_decoded = c1_bytes.decode('utf-8')
    c2_decoded = c2_bytes.decode('utf-8')
    json.loads(c1_decoded)
    print("OK return_1: Decodificacion UTF-8 y parse exitoso")
    json.loads(c2_decoded)
    print("OK return_2: Decodificacion UTF-8 y parse exitoso")
except Exception as e:
    print(f"ERROR en procesamiento: {e}")

print("\n=== RESUMEN DE DIFERENCIAS CLAVE ===")
print(f"1. Tamano: return_2 es {len(c2)/len(c1):.2f}x mas grande")
print(f"2. Items: return_2 tiene {len(d2['data'])-len(d1['data'])} items mas")
print(f"3. \\r\\n escapados: return_2 tiene {escaped_rn_2} secuencias, return_1 tiene {escaped_rn_1}")
print(f"4. Descripciones con newlines: return_2 tiene {len(descriptions_2)}, return_1 tiene {len(descriptions_1)}")
if escaped_rn_2 > 0:
    print(f"\n*** DIFERENCIA CRITICA: return_2 contiene {escaped_rn_2} secuencias \\r\\n escapadas")
    print("   Estas secuencias estan correctamente escapadas en JSON, pero podrian")
    print("   causar problemas durante la decodificacion HTTP si requests no maneja")
    print("   correctamente el encoding de respuestas grandes con caracteres especiales.")

