"""Ejecuta el notebook desde la raíz del proyecto; no escribe Landing."""
import os
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def main():
    root = Path(__file__).resolve().parents[1]
    os.environ.setdefault("PROJECT_ROOT", str(root))
    nb = nbformat.read(root / "notebooks/01_exploracion_fuentes.ipynb", as_version=4)
    runtime_cell = nbformat.v4.new_code_cell(
        "import platform\n"
        "print('Python', platform.python_version())\n"
        "import subprocess, os\n"
        "print(subprocess.run([os.path.join(os.environ['JAVA_HOME'], 'bin', 'java') "
        "if 'JAVA_HOME' in os.environ else 'java', '-version'], "
        "capture_output=True, text=True, check=True).stderr)"
    )
    # Los notebooks anteriores a nbformat 4.5 no admiten el campo id.
    if nb.nbformat_minor < 5:
        runtime_cell.pop("id", None)
    nb.cells.insert(0, runtime_cell)
    client = NotebookClient(
        nb, timeout=600, kernel_name="python3",
        resources={"metadata": {"path": str(root)}},
    )
    client.execute()
    # La misma raíz de evidencia que usa src.common.config en el notebook.
    evidence_dir = Path(os.environ.get("EVIDENCE_DIR", root / "evidence"))
    if not evidence_dir.is_absolute():
        evidence_dir = root / evidence_dir
    out = evidence_dir / "entrega1/01_exploracion_ejecutada.ipynb"
    out.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(nb, out)
    print(out)
    return out


if __name__ == "__main__":
    main()
