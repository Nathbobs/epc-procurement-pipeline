import time
import traceback


def run_stage(name, module_path):

    import importlib.util, sys

    print(f"\n{'─' * 50}")
    print(f"  STAGE: {name}")
    print(f"{'─' * 50}")

    start = time.time()
    try:
        spec = importlib.util.spec_from_file_location(name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        elapsed = time.time() - start
        print(f"{name} completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start
        print(f" {name} FAILED after {elapsed:.1f}s")
        print(f"  ERROR: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "═" * 50)
    print("  EPC PROCUREMENT INTELLIGENCE PIPELINE")
    print("  Starting full pipeline run...")
    print("═" * 50)

    pipeline_start = time.time()

    # Define stages in execution order
    stages = [
        ("Ingestion     (Raw → Bronze)",   "ingestion/ingest.py"),
        ("Transformation (Bronze → Silver)", "transformation/transform_silver.py"),
        ("Transformation (Silver → Gold)",   "transformation/transform_gold.py"),
        ("Warehouse     (Gold → Supabase)",  "warehouse/load_warehouse.py"),
        ("Quality Checks (Silver Layer)",    "quality/run_checks.py"),
    ]

    results = []
    for name, path in stages:
        success = run_stage(name, path)
        results.append((name, success))

        # Abort pipeline on critical stage failure
        if not success and "Quality" not in name:
            print(f"\n  ⚠️  Pipeline aborted at: {name}")
            print("  Fix the error above and re-run.\n")
            break

    # Pipeline summary
    total_elapsed = time.time() - pipeline_start
    print("\n" + "═" * 50)
    print("  PIPELINE SUMMARY")
    print("═" * 50)
    for name, success in results:
        status = " PASS" if success else " FAIL"
        print(f"  {status}  {name}")

    all_passed = all(s for _, s in results)
    print(f"\n  Total time : {total_elapsed:.1f}s")
    print(f"  Status     : {' ALL STAGES PASSED' if all_passed else ' PIPELINE FAILED'}")
    print("═" * 50 + "\n")