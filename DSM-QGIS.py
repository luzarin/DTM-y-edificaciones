"""
QGIS Processing Algorithm: LIDAR Workflow (Ground + Buildings → DSM + Filled)
Ejecuta pipeline completo: filtrado, fusión, rasterización y fillnodata
"""

from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessing, QgsProcessingAlgorithm, QgsProcessingMultiStepFeedback,
    QgsProcessingParameterFile, QgsProcessingParameterFolderDestination,
    QgsProcessingParameterNumber, QgsProcessingParameterBoolean
)
import subprocess
import json
import tempfile
from pathlib import Path


class LidarWorkflowProcessor(QgsProcessingAlgorithm):
    INPUT_FOLDER = 'INPUT_FOLDER'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'
    RESOLUTION = 'RESOLUTION'
    FILL_DISTANCE = 'FILL_DISTANCE'
    CLEANUP_TEMP = 'CLEANUP_TEMP'

    def tr(self, text):
        return QCoreApplication.translate('LidarWorkflowProcessor', text)

    def createInstance(self):
        return LidarWorkflowProcessor()

    def name(self):
        return 'lidar_workflow_processor'

    def displayName(self):
        return self.tr('LIDAR Workflow (Ground+Buildings→DSM)')

    def group(self):
        return self.tr('Point Cloud Processing')

    def groupId(self):
        return 'pointcloud_processing'

    def shortHelpString(self):
        return self.tr(
            'Procesa archivos LAZ/LAS:\n'
            '1. Filtra suelo (clase 2) y edificios (clase 6)\n'
            '2. Fusiona ambas clasificaciones\n'
            '3. Genera raster DSM con resolución configurable\n'
            '4. Rellena NoData usando gdal_fillnodata\n\n'
            'Requiere PDAL y GDAL instalados en el sistema.'
        )

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFile(
            self.INPUT_FOLDER,
            self.tr('Carpeta con archivos LAZ/LAS'),
            behavior=QgsProcessingParameterFile.Folder
        ))

        self.addParameter(QgsProcessingParameterFolderDestination(
            self.OUTPUT_FOLDER,
            self.tr('Carpeta de salida')
        ))

        self.addParameter(QgsProcessingParameterNumber(
            self.RESOLUTION,
            self.tr('Resolución del raster (metros)'),
            type=QgsProcessingParameterNumber.Double,
            defaultValue=0.5,
            minValue=0.1,
            maxValue=10.0
        ))

        self.addParameter(QgsProcessingParameterNumber(
            self.FILL_DISTANCE,
            self.tr('Distancia de relleno NoData (píxeles)'),
            type=QgsProcessingParameterNumber.Integer,
            defaultValue=75,
            minValue=1,
            maxValue=500
        ))

        self.addParameter(QgsProcessingParameterBoolean(
            self.CLEANUP_TEMP,
            self.tr('Eliminar archivos intermedios (suelo, edificios, merged)'),
            defaultValue=True
        ))

    def processAlgorithm(self, parameters, context, model_feedback):
        input_folder = Path(self.parameterAsFile(parameters, self.INPUT_FOLDER, context))
        output_folder = Path(self.parameterAsFileOutput(parameters, self.OUTPUT_FOLDER, context))
        resolution = self.parameterAsDouble(parameters, self.RESOLUTION, context)
        fill_distance = self.parameterAsInt(parameters, self.FILL_DISTANCE, context)
        cleanup = self.parameterAsBoolean(parameters, self.CLEANUP_TEMP, context)

        nodata_folder = output_folder / 'nodata_raster_final'
        temp_folder = output_folder / 'temp'
        nodata_folder.mkdir(parents=True, exist_ok=True)
        temp_folder.mkdir(parents=True, exist_ok=True)

        laz_files = list(input_folder.glob("*.laz")) + list(input_folder.glob("*.las"))
        if not laz_files:
            raise Exception("No se encontraron archivos LAZ/LAS en la carpeta")

        steps = len(laz_files) * 5 + 1
        feedback = QgsProcessingMultiStepFeedback(steps, model_feedback)

        raster_outputs = []
        current_step = 0

        for laz_file in laz_files:
            if feedback.isCanceled():
                break

            base_name = laz_file.stem
            feedback.pushInfo(f"\n{'='*50}\nProcesando: {laz_file.name}\n{'='*50}")

            output_suelo = output_folder / f"{base_name}_suelo.las"
            output_edificios = output_folder / f"{base_name}_edificios.las"
            output_merged = output_folder / f"{base_name}_merged.las"
            raster_output = output_folder / f"{base_name}_raster.tif"

            pipeline_files = []

            try:
                # 1. Filtrar suelo
                feedback.setCurrentStep(current_step)
                feedback.pushInfo("Filtrando suelo (clase 2)...")
                pipeline = self._create_filter_pipeline(laz_file, output_suelo, 2)
                pipeline_file = temp_folder / f"pipeline_suelo_{base_name}.json"
                pipeline_files.append(pipeline_file)
                if not self._run_pdal_pipeline(pipeline, pipeline_file, feedback):
                    continue
                current_step += 1

                # 2. Filtrar edificios
                feedback.setCurrentStep(current_step)
                feedback.pushInfo("Filtrando edificios (clase 6)...")
                pipeline = self._create_filter_pipeline(laz_file, output_edificios, 6)
                pipeline_file = temp_folder / f"pipeline_edificios_{base_name}.json"
                pipeline_files.append(pipeline_file)
                if not self._run_pdal_pipeline(pipeline, pipeline_file, feedback):
                    continue
                current_step += 1

                # 3. Fusionar
                feedback.setCurrentStep(current_step)
                feedback.pushInfo("Fusionando clasificaciones...")
                pipeline = self._create_merge_pipeline([output_suelo, output_edificios], output_merged)
                pipeline_file = temp_folder / f"pipeline_merge_{base_name}.json"
                pipeline_files.append(pipeline_file)
                if not self._run_pdal_pipeline(pipeline, pipeline_file, feedback):
                    continue
                current_step += 1

                # 4. Generar raster
                feedback.setCurrentStep(current_step)
                feedback.pushInfo("Generando raster DSM...")
                pipeline = self._create_raster_pipeline(output_merged, raster_output, resolution)
                pipeline_file = temp_folder / f"pipeline_raster_{base_name}.json"
                pipeline_files.append(pipeline_file)
                if not self._run_pdal_pipeline(pipeline, pipeline_file, feedback):
                    continue
                current_step += 1

                raster_outputs.append(raster_output)

                # 5. Limpiar intermedios si se solicitó
                feedback.setCurrentStep(current_step)
                if cleanup:
                    for f in [output_suelo, output_edificios, output_merged]:
                        if f.exists():
                            f.unlink()
                    feedback.pushInfo("Archivos intermedios eliminados")
                current_step += 1

                # Limpiar pipelines JSON
                for pf in pipeline_files:
                    if pf.exists():
                        pf.unlink()

            except Exception as e:
                feedback.reportError(f"Error procesando {laz_file.name}: {str(e)}")
                current_step += (5 - (current_step % 5))
                continue

        # Relleno de NoData
        feedback.setCurrentStep(current_step)
        if raster_outputs:
            feedback.pushInfo("\n" + "="*50)
            feedback.pushInfo("Iniciando relleno de NoData")
            feedback.pushInfo("="*50)

            fillnodata_cmd = self._detect_fillnodata()
            if fillnodata_cmd is None:
                feedback.reportError("gdal_fillnodata no encontrado. Instala GDAL Python utilities.")
            else:
                feedback.pushInfo(f"Usando: {' '.join(fillnodata_cmd)}")
                for raster_file in raster_outputs:
                    if feedback.isCanceled():
                        break
                    output_nodata = nodata_folder / f"{raster_file.stem}_filled.tif"
                    cmd = fillnodata_cmd + [
                        "-md", str(fill_distance),
                        "-si", "0",
                        str(raster_file),
                        str(output_nodata)
                    ]
                    if self._run_command(cmd, feedback):
                        feedback.pushInfo(f"✓ Rellenado: {output_nodata.name}")

        feedback.pushInfo(f"\nProceso completado: {len(raster_outputs)}/{len(laz_files)} archivos")
        return {
            self.OUTPUT_FOLDER: str(output_folder),
            'PROCESSED_COUNT': len(raster_outputs),
            'TOTAL_COUNT': len(laz_files)
        }

    def _create_filter_pipeline(self, input_file, output_file, classification):
        return {
            "pipeline": [
                str(input_file),
                {"type": "filters.range", "limits": f"Classification[{classification}:{classification}]"},
                str(output_file)
            ]
        }

    def _create_merge_pipeline(self, input_files, output_file):
        return {
            "pipeline": [
                *[str(f) for f in input_files],
                {"type": "filters.merge"},
                str(output_file)
            ]
        }

    def _create_raster_pipeline(self, input_file, output_file, resolution):
        return {
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

    def _run_pdal_pipeline(self, pipeline, pipeline_file, feedback):
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline, f, indent=2)
        cmd = ["pdal", "pipeline", str(pipeline_file)]
        return self._run_command(cmd, feedback)

    def _run_command(self, cmd, feedback):
        try:
            startupinfo = None
            if hasattr(subprocess, 'STARTUPINFO'):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
            
            result = subprocess.run(
                cmd, 
                check=True, 
                capture_output=True, 
                text=True, 
                timeout=300,
                startupinfo=startupinfo
            )
            return True
        except subprocess.CalledProcessError as e:
            feedback.reportError(f"Error: {e.stderr.strip() if e.stderr else 'Sin detalles'}")
            return False
        except subprocess.TimeoutExpired:
            feedback.reportError("Timeout: comando excedió 5 minutos")
            return False

    def _detect_fillnodata(self):
        opciones = [
            ["python", "-m", "osgeo_utils.gdal_fillnodata"],
            ["gdal_fillnodata"],
            ["gdal_fillnodata.py"]
        ]
        for cmd in opciones:
            try:
                startupinfo = None
                if hasattr(subprocess, 'STARTUPINFO'):
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    startupinfo.wShowWindow = subprocess.SW_HIDE
                
                subprocess.run(
                    cmd + ["--help"], 
                    check=True, 
                    capture_output=True, 
                    timeout=5,
                    startupinfo=startupinfo
                )
                return cmd
            except:
                continue
        return None


def classFactory(iface=None):
    return LidarWorkflowProcessor()