del /s /q dist/
del /s /q build
python setup.py sdist build
python -m twine upload dist/*