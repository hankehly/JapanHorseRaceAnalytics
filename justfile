[doc("Show compiled SQL code for failed dbt models")]
show_failed_sql:
    @jq -r '.results[] | select(.status=="fail").compiled_code' target/run_results.json

[doc("Print the netkeiba link for a given race key")]
netkeiba_link *args:
    @uv run python scripts/netkeiba_link.py {{args}}
