mkdir -p /home/udflib/python_package
cd /home/udflib/python_package
git clone https://github.com/mpdevs/mplib.git

yum install postgresql-devel-8.4.18-1.el6_4.x86_64
yum install postgresql-8.4.18-1.el6_4.x86_64
yum install postgresql-libs-8.4.18-1.el6_4.x86_64
yum install cyrus-sasl-lib-2.1.23-13.el6_3.1.x86_64
yum install cyrus-sasl-2.1.23-13.el6_3.1.x86_64
yum install cyrus-sasl-devel-2.1.23-13.el6_3.1.x86_64
yum install cyrus-sasl-gssapi-2.1.23-13.el6_3.1.x86_64
yum install cyrus-sasl-plain-2.1.23-13.el6_3.1.x86_64

pip install -r requirements.txt
