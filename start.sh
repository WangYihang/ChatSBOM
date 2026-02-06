uv run python -m chatsbom collect --language go --min-stars 10000
uv run python -m chatsbom download --language go
uv run python -m chatsbom convert --language go
uv run python -m chatsbom index --language go
uv run python -m chatsbom query gin --language go
uv run python -m chatsbom status
uv run python -m chatsbom chat
