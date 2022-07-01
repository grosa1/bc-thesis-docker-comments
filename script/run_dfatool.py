import psycopg2
import subprocess
import os
import shutil


out_dir = 'dockerfiles_temp'
if not os.path.exists(out_dir):
   os.mkdir(out_dir)

con = psycopg2.connect(database="", user="", password="", host="127.0.0.1", port="5432")
cur = con.cursor('server_cursor')
cur.execute("SELECT sha1, content FROM blob WHERE validcmds = TRUE")

while True:
    # consume result over a series of iterations
    # with each iteration fetching 2000 records
    records = cur.fetchmany(size=2000)
    if not records:
        break

    filelist = list()
    for row in records:
        fpath = os.path.join(out_dir, row[0], "Dockerfile")
        filelist.append(fpath)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(os.path.join(fpath), "w") as out:
            out.write(row[1])
    process = subprocess.Popen(f"ruby repository_parser.rb -dir {os.path.abspath(out_dir)} 0 --dbmongo 127.0.0.1 27017 --login dockeruser dockerusermongo2021_", shell=True, stdout=subprocess.PIPE)
    process.wait()

    for f in filelist:
        shutil.rmtree(os.path.dirname(f))

cur.close() # don't forget to cleanup
con.close()

shutil.rmtree(out_dir)

print("Operation done successfully")
