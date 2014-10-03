c = get_config()
c.NotebookApp.certfile = u'/root/mycert.pem'
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 8888
PWDFILE="/root/.ipython/profile_default/nbpasswd.txt"
c.NotebookApp.password = open(PWDFILE).read().strip()
