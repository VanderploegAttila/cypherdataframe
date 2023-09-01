testing 123
usr : avan
pass :10 ...
venvs
rm -r dist
python setup.py sdist bdist_wheel
twine upload dist/*
pip uninstall cypherdataframe
pip install cypherdataframe --upgrade
