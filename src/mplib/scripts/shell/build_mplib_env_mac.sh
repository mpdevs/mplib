cd /home/udflib/python_package/mplib
pip install -r requirements.txt

conda install nomkl numpy scipy scikit-learn numexpr
conda remove mkl mkl-service
