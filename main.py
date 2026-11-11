from pathlib import Path
import subprocess


def main() -> None:
    """Start the repository's local Airflow development stack."""
    project_dir = Path(__file__).parent / "airflow_dbt_project"
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build"],
        cwd=project_dir,
        check=True,
    )


if __name__ == "__main__":
    main()
