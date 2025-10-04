"""
Workflow de procesamiento LIDAR (Funciona en QGIS y standalone)
- Filtrado de puntos (suelo y edificios)
- Fusión de clasificaciones
- Creación de DSM raster
- Relleno de datos faltantes
"""

import os
import json
import subprocess
from pathlib import Path

# ============================================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================================
INPUT_FOLDER = Path("C:/Users/lucas/Downloads/toledo3")
OUTPUT_FOLDER = Path("C:/Users/lucas/Downloads/toledo3/resultados")
NODATA_FOLDER = Path("C:/Users/lucas/Downloads/toledo3/resultados/nodata_rasters")
TEMP_FOLDER = Path("C:/Users/lucas/Downloads/toledo3/temp")

# Crear carpetas si no existen
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
NODATA_FOLDER.mkdir(parents=True, exist_ok=True)
TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

# ============================================================================
# CONFIGURACIÓN DE PROCESAMIENTO
# ============================================================================
RESOLUTION = 0.5  # Resolución del raster en metros
FILL_DISTANCE = 75  # Distancia para rellenar NoData

# ============================================================================
# LISTA DE ARCHIVOS
# ============================================================================
laz_files = list(INPUT_FOLDER.glob("*.laz"))
print(f"📦 Encontrados {len(laz_files)} archivos LAZ para procesar\n")

if not laz_files:
    print("⚠️  No se encontraron archivos .laz en la carpeta de entrada")
    exit()

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================
def run_command(cmd, description):
    """Ejecuta un comando y maneja errores"""
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"    ❌ Error en {description}")
        if e.stderr:
            print(f"    {e.stderr.strip()}")
        return False

def create_pipeline_json(input_file, output_file, classification):
    """Crea un pipeline JSON de PDAL para filtrar por clasificación"""
    pipeline = {
        "pipeline": [
            str(input_file),
            {
                "type": "filters.range",
                "limits": f"Classification[{classification}:{classification}]"
            },
            str(output_file)
        ]
    }
    return pipeline

def create_merge_pipeline_json(input_files, output_file):
    """Crea un pipeline JSON de PDAL para fusionar archivos"""
    pipeline = {
        "pipeline": [
            *[str(f) for f in input_files],
            {
                "type": "filters.merge"
            },
            str(output_file)
        ]
    }
    return pipeline

def create_raster_pipeline_json(input_file, output_file, resolution):
    """Crea un pipeline JSON de PDAL para generar raster"""
    pipeline = {
        "pipeline": [
            str(input_file),
            {
                "type": "writers.gdal",
                "filename": str(output_file),
                "resolution": resolution,
                "output_type": "max",
                "gdalopts": "COMPRESS=DEFLATE,TILED=YES"
            }
        ]
    }
    return pipeline

def run_pdal_pipeline(pipeline, pipeline_file, description):
    """Ejecuta un pipeline de PDAL"""
    # Escribir el pipeline a un archivo JSON temporal
    with open(pipeline_file, 'w') as f:
        json.dump(pipeline, f, indent=2)
    
    # Ejecutar PDAL con el pipeline
    cmd = ["pdal", "pipeline", str(pipeline_file)]
    return run_command(cmd, description)

# ============================================================================
# PROCESAMIENTO
# ============================================================================
raster_outputs = []

for i, laz_file in enumerate(laz_files, 1):
    print(f"{'='*70}")
    print(f"🔄 [{i}/{len(laz_files)}] Procesando: {laz_file.name}")
    print(f"{'='*70}")
    
    base_name = laz_file.stem
    
    # Archivos intermedios
    output_suelo = OUTPUT_FOLDER / f"{base_name}_suelo.las"
    output_edificios = OUTPUT_FOLDER / f"{base_name}_edificios.las"
    output_merged = OUTPUT_FOLDER / f"{base_name}_merged.las"
    raster_output = OUTPUT_FOLDER / f"{base_name}_raster.tif"
    
    # Archivos de pipeline temporales
    pipeline_suelo = TEMP_FOLDER / f"pipeline_suelo_{base_name}.json"
    pipeline_edificios = TEMP_FOLDER / f"pipeline_edificios_{base_name}.json"
    pipeline_merge = TEMP_FOLDER / f"pipeline_merge_{base_name}.json"
    pipeline_raster = TEMP_FOLDER / f"pipeline_raster_{base_name}.json"
    
    try:
        # --- 1. FILTRADO: SUELO (Clasificación 2) ---
        print("  🌍 Filtrando puntos de suelo (Clase 2)...")
        pipeline = create_pipeline_json(laz_file, output_suelo, 2)
        if not run_pdal_pipeline(pipeline, pipeline_suelo, "filtrado de suelo"):
            continue
        
        # --- 2. FILTRADO: EDIFICIOS (Clasificación 6) ---
        print("  🏢 Filtrando puntos de edificios (Clase 6)...")
        pipeline = create_pipeline_json(laz_file, output_edificios, 6)
        if not run_pdal_pipeline(pipeline, pipeline_edificios, "filtrado de edificios"):
            continue
        
        # --- 3. FUSIÓN DE CLASIFICACIONES ---
        print("  🔗 Fusionando suelo + edificios...")
        pipeline = create_merge_pipeline_json([output_suelo, output_edificios], output_merged)
        if not run_pdal_pipeline(pipeline, pipeline_merge, "fusión"):
            continue
        
        # --- 4. EXPORTAR A RASTER (DSM) ---
        print("  📊 Generando raster DSM...")
        pipeline = create_raster_pipeline_json(output_merged, raster_output, RESOLUTION)
        if not run_pdal_pipeline(pipeline, pipeline_raster, "generación de raster"):
            continue
        
        raster_outputs.append(raster_output)
        print(f"✅ Completado: {laz_file.name}\n")
        
    except Exception as e:
        print(f"❌ Error procesando {laz_file.name}: {e}\n")
        continue

# ============================================================================
# DETECTAR COMANDO GDAL_FILLNODATA
# ============================================================================
def get_fillnodata_command():
    """Detecta cómo ejecutar gdal_fillnodata en el sistema"""
    # Probar diferentes opciones
    opciones = [
        ["python", "-m", "osgeo_utils.gdal_fillnodata"],  # Conda/pip
        ["gdal_fillnodata"],  # Comando directo
        ["gdal_fillnodata.py"],  # Script directo
    ]
    
    for cmd in opciones:
        try:
            test_cmd = cmd + ["--help"]
            subprocess.run(test_cmd, check=True, capture_output=True, timeout=5)
            return cmd
        except:
            continue
    
    return None

# ============================================================================
# RELLENO DE DATOS FALTANTES
# ============================================================================
if raster_outputs:
    print(f"\n{'='*70}")
    print(f"🔧 Iniciando relleno de datos faltantes (fillnodata)")
    print(f"{'='*70}\n")
    
    # Detectar comando gdal_fillnodata
    fillnodata_cmd = get_fillnodata_command()
    
    if fillnodata_cmd is None:
        print("❌ No se pudo encontrar gdal_fillnodata en el sistema")
        print("   Instala GDAL Python utilities: pip install gdal")
    else:
        print(f"✓ Usando comando: {' '.join(fillnodata_cmd)}\n")
        
        for i, raster_file in enumerate(raster_outputs, 1):
            print(f"🔄 [{i}/{len(raster_outputs)}] Rellenando: {raster_file.name}")
            
            output_nodata = NODATA_FOLDER / f"{raster_file.stem}_nodata.tif"
            
            cmd_fillnodata = fillnodata_cmd + [
                "-md", str(FILL_DISTANCE),
                "-si", "0",
                str(raster_file),
                str(output_nodata)
            ]
            
            if run_command(cmd_fillnodata, "relleno de NoData"):
                print(f"✅ Completado: {output_nodata.name}\n")
            else:
                print(f"⚠️  No se pudo rellenar {raster_file.name}\n")

# ============================================================================
# LIMPIEZA DE ARCHIVOS TEMPORALES
# ============================================================================
print("🧹 Limpiando archivos temporales...")
for temp_file in TEMP_FOLDER.glob("*.json"):
    temp_file.unlink()

# ============================================================================
# FIN
# ============================================================================
print(f"{'='*70}")
print("🎉 ¡Proceso finalizado exitosamente!")
print(f"📁 Resultados en: {OUTPUT_FOLDER}")
print(f"📁 Rasters rellenados en: {NODATA_FOLDER}")
print(f"📊 Archivos procesados: {len(raster_outputs)}/{len(laz_files)}")
print(f"{'='*70}")