start_mlflow_server:
	uv run -- mlflow server \
		--host 127.0.0.1 \
		--port 8080 \
		--backend-store-uri postgresql://admin:admin@127.0.0.1:5432/mlflow \
		--default-artifact-root ./mlruns
