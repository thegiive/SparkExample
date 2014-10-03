cd /root/SparkExample-master/
cp mycert.pem /root
ipython profile create default
cp nbpasswd.txt /root/.ipython/profile_default/nbpasswd.txt
cp ipython/ipython_notebook_config.py /root/.ipython/profile_default/ipython_notebook_config.py
cp ipython/00-pyspark-setup.py /root/.ipython/profile_default/startup/00-pyspark-setup.py
echo 'https://'`ec2-metadata -p | awk -F" " '{print $2}'`':8888/'
export AWS_ACCESS_KEY_ID=AKIAJVERJHTPLWB33UIA AWS_SECRET_ACCESS_KEY=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
cd /root/spark ; IPYTHON_OPTS="notebook" ./bin/pyspark
