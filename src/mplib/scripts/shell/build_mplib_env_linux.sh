yum install postgresql-devel postgresql postgresql-libs
yum install cyrus-sasl-lib cyrus-sasl-plain cyrus-sasl cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain
yum install mysql mysql-devel mysql-common mysql-libs

cd /home/udflib/python_package/mplib
pip install -r requirements.txt
cd /home/udflib/python_package/pykylin
python setup.py bdist_egg
python setup.py install

conda install nomkl numpy scipy scikit-learn numexpr
conda remove mkl mkl-service
