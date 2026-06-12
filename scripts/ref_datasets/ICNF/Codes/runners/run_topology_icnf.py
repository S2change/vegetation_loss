import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.core.topology_icnf import process_icnf_folder


def main() -> None:
    data_dir = PROJECT_ROOT / "Data"
    results_dir = PROJECT_ROOT / "Results" / "Topologia_revisado"
    results_dir.mkdir(parents=True, exist_ok=True)

    print("Project root:       ", PROJECT_ROOT)
    print("Input data folder:  ", data_dir)
    print("Results folder:     ", results_dir)

    process_icnf_folder(
        icnf_folder=str(data_dir),
        out_folder=str(results_dir),
        pattern="ardida_*.shp",
        min_overlap_area=1.0,
    )


if __name__ == "__main__":
    main()
