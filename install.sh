cp mycert.pem /root
ipython profile create default
cp nbpasswd.txt /root/.ipython/profile_default/nbpasswd.txt
cp ipython_notebook_config.py /root/.ipython/profile_default/ipython_notebook_config.py
cp 00-pyspark-setup.py /root/.ipython/profile_default/startup/00-pyspark-setup.py
echo 'https://'`hostname`':8888/'
ipython notebook
