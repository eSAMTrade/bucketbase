del .\dist\*.whl
del .\dist\*.gz
uv build
uv run twine upload dist/*.whl --verbose
