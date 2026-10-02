"""Ejecuta el notebook desde la raíz del proyecto; no escribe Landing."""
from pathlib import Path
import os, nbformat
from nbclient import NotebookClient
root = Path(__file__).resolve().parents[1]
os.environ.setdefault('PROJECT_ROOT', str(root))
nb = nbformat.read(root / 'notebooks/01_exploracion_fuentes.ipynb', as_version=4)
nb.cells.insert(0, nbformat.v4.new_code_cell("import platform\nprint('Python', platform.python_version())\nimport subprocess, os\nprint(subprocess.run([os.path.join(os.environ['JAVA_HOME'], 'bin', 'java') if 'JAVA_HOME' in os.environ else 'java', '-version'], capture_output=True, text=True, check=True).stderr)"))
client = NotebookClient(nb, timeout=600, kernel_name='python3', resources={'metadata': {'path': str(root)}})
client.execute()
out = root / 'evidence/entrega1/01_exploracion_ejecutada.ipynb'
out.parent.mkdir(parents=True, exist_ok=True)
nbformat.write(nb, out)
print(out)
