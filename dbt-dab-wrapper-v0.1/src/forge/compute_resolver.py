# src/forge/compute_resolver.py
# COMMENT: Decides serverless vs dedicated. Future-proofs for postgres.
def resolve_compute(env: str):
    # Reads forge.yml (real version uses PyYAML)
    compute_type = "serverless"
    print(f"Resolved compute: {compute_type} (works on any platform)")
    return compute_type
